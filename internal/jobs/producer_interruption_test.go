package jobs_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rvben/shinyhub/internal/db"
	"github.com/rvben/shinyhub/internal/jobs"
	"github.com/rvben/shinyhub/internal/process"
)

func TestProducerInterruptionRespectsPhysicalWriteBoundary(t *testing.T) {
	for _, action := range []string{"operator_cancel", "owner_handoff"} {
		for _, writing := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/writing=%t", action, writing), func(t *testing.T) {
				store, app, scheduleID := publicationHandoffFixture(t)
				sharedData := t.TempDir()
				publisher := publicationHandoffManager(t, store, sharedData, &fakeRuntime{})
				baselineID, err := publisher.Run(scheduleID, "manual", nil)
				if err != nil {
					t.Fatal(err)
				}
				waitPublicationRun(t, store, baselineID, "succeeded")
				assertRecoveryFenceAvailable(t, publisher, []int64{app.ID})
				runtime := &fakeRuntime{block: make(chan struct{}), entered: make(chan struct{}, 1)}
				writer := publicationHandoffManager(t, store, sharedData, runtime)
				var cleanups atomic.Int64
				writer.SetUnsafePublicationHandler(func(int64, string, string) { cleanups.Add(1) })
				var releaseOnce sync.Once
				releaseConsumer := func() {}
				if !writing {
					// A consumer on another manager makes physical execution
					// impossible, regardless of how soon cancellation arrives.
					release, err := publisher.AcquireConsumerBootGate(app.ID)
					if err != nil {
						t.Fatal(err)
					}
					releaseConsumer = func() { releaseOnce.Do(release) }
					t.Cleanup(releaseConsumer)
				}
				runID, err := writer.Run(scheduleID, "manual", nil)
				if err != nil {
					t.Fatal(err)
				}
				if writing {
					select {
					case <-runtime.entered:
					case <-time.After(5 * time.Second):
						t.Fatal("producer never reached physical execution")
					}
				}
				terminal := "cancelled"
				if action == "operator_cancel" {
					if err := writer.Cancel(runID); err != nil {
						t.Fatal(err)
					}
				} else {
					terminal = "interrupted"
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					err := writer.InterruptAndDrain(ctx)
					cancel()
					if err != nil {
						t.Fatalf("owner handoff did not join admitted producer: %v", err)
					}
					if _, err := writer.Run(scheduleID, "manual", nil); !errors.Is(err, jobs.ErrManagerStopped) {
						t.Fatalf("retired owner still admits work: %v", err)
					}
				}
				// Cancellation must finish even with the consumer fence held.
				waitPublicationRun(t, store, runID, terminal)
				releaseConsumer()
				assertRecoveryFenceAvailable(t, publisher, []int64{app.ID})
				run, err := store.GetScheduleRun(runID)
				if err != nil || run.ExitCode != nil {
					t.Fatalf("cancellation invented a process exit: run=%+v err=%v", run, err)
				}
				runtime.mu.Lock()
				calls := runtime.calls
				runtime.mu.Unlock()
				wantCalls, wantCleanups, wantStatus := 0, int64(0), "running"
				if writing {
					wantCalls, wantCleanups, wantStatus = 1, 1, "failed"
				}
				if calls != wantCalls || cleanups.Load() != wantCleanups {
					t.Fatalf("physical calls=%d cleanup calls=%d; want %d/%d", calls, cleanups.Load(), wantCalls, wantCleanups)
				}
				state, err := store.GetAppByID(app.ID)
				if err != nil || state.Status != wantStatus {
					t.Fatalf("interruption changed availability incorrectly: app=%+v err=%v", state, err)
				}
				if writing && (!strings.Contains(state.LastError, fmt.Sprintf("producer run %d", runID)) || !strings.Contains(state.LastError, terminal)) {
					t.Fatalf("interrupted write lost its diagnosis: %s", state.LastError)
				}
				if !writing && state.LastError != "" {
					t.Fatalf("queued cancellation invented failure diagnosis: %s", state.LastError)
				}
				blocked, err := store.AppCompatibilityQuarantined(app.ID)
				if err != nil || blocked != writing {
					t.Fatalf("write boundary quarantine=%t err=%v; want %t", blocked, err, writing)
				}
				assertInterruptionPublication(t, store, app.ID, baselineID, 1)
				// A successor can repair both cancellation classes, including an
				// owner-handoff interruption, without reviving the old manager.
				repairID, err := publisher.Run(scheduleID, "manual", nil)
				if err != nil {
					t.Fatal(err)
				}
				waitPublicationRun(t, store, repairID, "succeeded")
				assertRecoveryFenceAvailable(t, publisher, []int64{app.ID})
				state, err = store.GetAppByID(app.ID)
				if err != nil || state.Status != "running" || state.LastError != "" {
					t.Fatalf("successor repair did not restore availability: app=%+v err=%v", state, err)
				}
				blocked, err = store.AppCompatibilityQuarantined(app.ID)
				if err != nil || blocked {
					t.Fatalf("successor repair retained uncertainty: %t %v", blocked, err)
				}
				assertInterruptionPublication(t, store, app.ID, repairID, 2)
				count, err := store.CountAuditEvents("app_status_changed")
				if err != nil || count != 2*wantCleanups {
					t.Fatalf("interruption transition audit count=%d err=%v; want %d", count, err, 2*wantCleanups)
				}
			})
		}
	}
}

func assertInterruptionPublication(t *testing.T, store *db.Store, appID, runID, generation int64) {
	t.Helper()
	publication, err := store.GetAppDataPublication(appID)
	if err != nil || publication.ScheduleRunID != runID || publication.Generation != generation {
		t.Fatalf("interruption changed publication: %+v err=%v; want run=%d generation=%d", publication, err, runID, generation)
	}
}

func TestQueuedProducerRevalidatesDataIdentityWithoutRejectingCronEdits(t *testing.T) {
	for _, edit := range []string{"command", "inputs", "cron"} {
		t.Run(edit, func(t *testing.T) {
			store, app, scheduleID := publicationHandoffFixture(t)
			sharedData := t.TempDir()
			publisher := publicationHandoffManager(t, store, sharedData, &fakeRuntime{})
			baselineID, err := publisher.Run(scheduleID, "manual", nil)
			if err != nil {
				t.Fatal(err)
			}
			waitPublicationRun(t, store, baselineID, "succeeded")
			assertRecoveryFenceAvailable(t, publisher, []int64{app.ID})
			runtime := &fakeRuntime{exitInfo: process.ExitInfo{Code: 0}}
			writer := publicationHandoffManager(t, store, sharedData, runtime)
			release, err := publisher.AcquireConsumerBootGate(app.ID)
			if err != nil {
				t.Fatal(err)
			}
			var once sync.Once
			releaseConsumer := func() { once.Do(release) }
			t.Cleanup(releaseConsumer)
			queuedID, err := writer.Run(scheduleID, "manual", nil)
			if err != nil {
				t.Fatal(err)
			}
			queued, err := store.GetScheduleRun(queuedID)
			if err != nil || queued.Status != "running" || queued.ProducerCommandJSON != `["producer"]` {
				t.Fatalf("admission did not snapshot original producer: run=%+v err=%v", queued, err)
			}
			params := db.UpdateScheduleParams{}
			switch edit {
			case "command":
				command := `["replacement-producer"]`
				params.CommandJSON = &command
			case "inputs":
				deployment, err := store.GetServingDeployment(app.ID)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(deployment.BundleDir, "input.csv"), []byte("new producer input\n"), 0600); err != nil {
					t.Fatal(err)
				}
				inputs := `["input.csv"]`
				params.InputsJSON = &inputs
			case "cron":
				cron := "0 6 * * *"
				params.CronExpr = &cron
			}
			if err := store.UpdateSchedule(scheduleID, params); err != nil {
				t.Fatal(err)
			}
			releaseConsumer()
			terminal, wantCalls := "superseded", 0
			if edit == "cron" {
				terminal, wantCalls = "succeeded", 1
			}
			waitPublicationRun(t, store, queuedID, terminal)
			assertRecoveryFenceAvailable(t, publisher, []int64{app.ID})
			runtime.mu.Lock()
			calls := runtime.calls
			runtime.mu.Unlock()
			if calls != wantCalls {
				t.Fatalf("%s edit: obsolete producer executed %d times; want %d", edit, calls, wantCalls)
			}
			if edit == "cron" {
				assertInterruptionPublication(t, store, app.ID, queuedID, 2)
			} else {
				assertInterruptionPublication(t, store, app.ID, baselineID, 1)
			}
			state, err := store.GetAppByID(app.ID)
			if err != nil || state.Status != "running" || state.LastError != "" {
				t.Fatalf("declaration edit invented an outage: app=%+v err=%v", state, err)
			}
			blocked, err := store.AppCompatibilityQuarantined(app.ID)
			if err != nil || blocked {
				t.Fatalf("non-writing supersession left uncertainty: %t %v", blocked, err)
			}
			// The current declaration must remain executable after the stale
			// admission finishes, rather than leaving the schedule wedged.
			currentID, err := writer.Run(scheduleID, "manual", nil)
			if err != nil {
				t.Fatal(err)
			}
			waitPublicationRun(t, store, currentID, "succeeded")
			assertRecoveryFenceAvailable(t, publisher, []int64{app.ID})
			generation := int64(2)
			if edit == "cron" {
				generation = 3
			}
			assertInterruptionPublication(t, store, app.ID, currentID, generation)
			runtime.mu.Lock()
			command := append([]string(nil), runtime.lastParams.Command...)
			runtime.mu.Unlock()
			wantCommand := "producer"
			if edit == "command" {
				wantCommand = "replacement-producer"
			}
			if len(command) != 1 || command[0] != wantCommand {
				t.Fatalf("current run executed obsolete command: %v; want %s", command, wantCommand)
			}
		})
	}
}

package cli

import (
	"bytes"
	"context"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestBundleUploadReplay(t *testing.T) {
	payload := bytes.Repeat([]byte("compressed bundle bytes\x00"), 4000)
	req, err := newBundleUploadRequest(context.Background(), "http://example.test/deploy", payload)
	if err != nil {
		t.Fatal(err)
	}
	first, err := io.ReadAll(req.Body)
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(first)) != req.ContentLength {
		t.Fatalf("length=%d, declared=%d", len(first), req.ContentLength)
	}
	replay, err := req.GetBody()
	if err != nil {
		t.Fatal(err)
	}
	defer replay.Close()
	second, err := io.ReadAll(replay)
	if err != nil || !bytes.Equal(first, second) {
		t.Fatalf("replay differs: %v", err)
	}
}

func TestBundleUploadRedirect(t *testing.T) {
	for _, status := range []int{http.StatusTemporaryRedirect, http.StatusPermanentRedirect} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			payload := bytes.Repeat([]byte("ZIP bytes\x00"), 4096)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.ContentLength <= int64(len(payload)) || len(r.TransferEncoding) != 0 {
					t.Errorf("method=%s length=%d encoding=%v", r.Method, r.ContentLength, r.TransferEncoding)
				}
				if r.URL.Path == "/redirect" {
					// Drain the first attempt: replay must work even after consumption.
					_, _ = io.Copy(io.Discard, r.Body)
					http.Redirect(w, r, "/upload", status)
					return
				}

				if r.Header.Get("Authorization") != "Bearer test-token" {
					t.Error("lost auth on same-host redirect")
				}
				if err := r.ParseMultipartForm(1 << 20); err != nil {
					t.Error(err)
					w.WriteHeader(400)
					return
				}
				defer r.MultipartForm.RemoveAll()
				file, header, err := r.FormFile("bundle")
				if err != nil {
					t.Error(err)
					w.WriteHeader(400)
					return
				}
				defer file.Close()
				got, err := io.ReadAll(file)
				if err != nil || !bytes.Equal(got, payload) || header.Filename != "bundle.zip" {
					t.Errorf("incorrect upload: %v", err)
				}
				w.WriteHeader(http.StatusAccepted)
			}))
			defer srv.Close()
			req, err := newBundleUploadRequest(context.Background(), srv.URL+"/redirect", payload)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", "Bearer test-token")
			resp, err := streamClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusAccepted {
				t.Fatalf("status=%s", resp.Status)
			}
		})
	}
}

func BenchmarkBundleUpload(b *testing.B) {
	payload := bytes.Repeat([]byte("x"), 8<<20)
	for _, buffered := range []bool{true, false} {
		name := "streamed"
		if buffered {
			name = "buffered-baseline"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))
			for b.Loop() {
				var req *http.Request
				var err error
				if buffered {
					var body bytes.Buffer
					w := multipart.NewWriter(&body)
					part, e := w.CreateFormFile("bundle", "bundle.zip")
					if e != nil {
						b.Fatal(e)
					}
					if _, e = io.Copy(part, bytes.NewReader(payload)); e != nil {
						b.Fatal(e)
					}
					if e = w.Close(); e != nil {
						b.Fatal(e)
					}
					req, err = http.NewRequest(http.MethodPost, "http://example.test/deploy", &body)
					if err == nil {
						req.Header.Set("Content-Type", w.FormDataContentType())
					}
				} else {
					req, err = newBundleUploadRequest(context.Background(), "http://example.test/deploy", payload)
				}
				if err != nil {
					b.Fatal(err)
				}
				if _, err = io.Copy(io.Discard, req.Body); err != nil {
					b.Fatal(err)
				}
				req.Body.Close()
			}
		})
	}
}

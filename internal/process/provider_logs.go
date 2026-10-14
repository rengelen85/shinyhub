package process

import (
	"context"
	"errors"
	"github.com/rvben/shinyhub/internal/nativebroker"
)

// ExternalLogReaders routes persisted log handoffs to their configured provider.
type ExternalLogReaders map[string]ExternalLogReader

func (readers ExternalLogReaders) Supports(provider string) bool { return readers[provider] != nil }

func (readers ExternalLogReaders) Read(ctx context.Context, details ExternalLogs, cursor string, limit int32) (ExternalLogPage, error) {
	reader := readers[details.Provider]
	if reader == nil {
		return ExternalLogPage{}, errors.New("log provider is not configured")
	}
	return reader.Read(ctx, details, cursor, limit)
}

// NewJournalLogReader reconnects to an existing broker after controller restart.
func NewJournalLogReader(socket string) ExternalLogReader {
	return &SystemdRuntime{client: nativebroker.Client{Socket: socket}}
}

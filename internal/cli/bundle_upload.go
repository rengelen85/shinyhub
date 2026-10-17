package cli

import (
	"bytes"
	"context"
	"io"
	"mime/multipart"
	"net/http"
)

// newBundleUploadRequest wraps an already compressed ZIP without copying it.
// Keep Content-Length and GetBody so proxies and 307/308 redirects behave as
// they did with a fully buffered multipart body. bundle must remain immutable.
func newBundleUploadRequest(ctx context.Context, url string, bundle []byte) (*http.Request, error) {
	var envelope bytes.Buffer
	w := multipart.NewWriter(&envelope)
	if _, err := w.CreateFormFile("bundle", "bundle.zip"); err != nil {
		return nil, err
	}
	headerLen := envelope.Len()
	if err := w.Close(); err != nil {
		return nil, err
	}
	framing := envelope.Bytes()
	newBody := func() io.ReadCloser {
		return io.NopCloser(io.MultiReader(
			bytes.NewReader(framing[:headerLen]), bytes.NewReader(bundle), bytes.NewReader(framing[headerLen:]),
		))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, newBody())
	if err != nil {
		return nil, err
	}
	req.ContentLength = int64(len(framing)) + int64(len(bundle))
	req.GetBody = func() (io.ReadCloser, error) { return newBody(), nil }
	req.Header.Set("Content-Type", w.FormDataContentType())
	return req, nil
}

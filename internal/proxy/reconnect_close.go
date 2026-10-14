package proxy

import (
	"bytes"
	"time"
)

// closeForReconnect sends WebSocket 1012 only at a complete outbound frame
// boundary. A partial frame or slow transport falls back to closing the socket;
// injecting a control frame into an unfinished data frame would corrupt it.
func (c *trackedConn) closeForReconnect(reason string) error {
	_ = c.Conn.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
	c.writeMu.Lock()
	if !c.writeFrames.invalid && c.writeFrames.have == 0 && c.writeFrames.remaining == 0 && len(c.writeHeader) == 0 {
		message := "server upgraded; reconnect"
		frame := append([]byte{0x88, byte(len(message) + 2), 0x03, 0xf4}, []byte(message)...)
		if _, err := c.Conn.Write(frame); err == nil && c.session != nil {
			c.session.mu.Lock()
			code := uint16(1012)
			c.session.closedBy, c.session.closeCode, c.session.closeReason = "proxy", &code, reason
			c.session.mu.Unlock()
		}
	}
	c.writeMu.Unlock()
	return c.closeWithReason(reason)
}

func (c *trackedConn) observeOutbound(p []byte) {
	if !c.writeStarted {
		c.writeStarted = true
		if len(p) > 0 && p[0] == 'H' {
			c.writeHeader = make([]byte, 0, 256)
		}
	}
	if c.writeHeader != nil {
		c.writeHeader = append(c.writeHeader, p...)
		if index := bytes.Index(c.writeHeader, []byte("\r\n\r\n")); index >= 0 {
			p = c.writeHeader[index+4:]
			c.writeFrames.feed(p, false, func(*uint16, string) {})
			c.writeHeader = nil
		} else if len(c.writeHeader) > 64*1024 {
			c.writeFrames.invalid = true
			c.writeHeader = nil
		}
		return
	}
	c.writeFrames.feed(p, false, func(*uint16, string) {})
}

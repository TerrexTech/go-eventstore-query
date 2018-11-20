package mock

import (
	"io"

	tlog "github.com/TerrexTech/go-logtransport/log"
)

// Logger mocks the Logger interface required by service.
// Currently, as per requirements, this is no-op to just prevent nil-pointer errors.
type Logger struct{}

// D mocks function D of go-logtransport/log package
func (l *Logger) D(entry tlog.Entry, data ...interface{}) {
	// No-op
}

// E mocks function E of go-logtransport/log package
func (l *Logger) E(entry tlog.Entry, data ...interface{}) {
	// No-op
}

// F mocks function F of go-logtransport/log package
func (l *Logger) F(entry tlog.Entry, data ...interface{}) {
	// No-op
}

// I mocks function I of go-logtransport/log package
func (l *Logger) I(entry tlog.Entry, data ...interface{}) {
	// No-op
}

// DisableOutput mocks function DisableOutput of go-logtransport/log package
func (l *Logger) DisableOutput() {
	// No-op
}

// EnableOutput mocks function EnableOutput of go-logtransport/log package
func (l *Logger) EnableOutput() {
	// No-op
}

// SetArrayThreshold mocks function SetArrayThreshold of go-logtransport/log package
func (l *Logger) SetArrayThreshold(threshold int) {
	// No-op
}

// SetAction mocks function SetAction of go-logtransport/log package
func (l *Logger) SetAction(action string) {
	// No-op
}

// SetOutput mocks function SetOutput of go-logtransport/log package
func (l *Logger) SetOutput(w io.Writer) {
	// No-op
}

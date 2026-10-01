package workflow

import (
	"fmt"
	"io"
	"log"
	"os"
)

// stdLogger adapts the standard library logger.
type stdLogger struct {
	l *log.Logger
}

func newLogger(w io.Writer) Logger {
	if w == nil {
		w = os.Stdout
	}
	return &stdLogger{l: log.New(w, "[workflow] ", log.LstdFlags|log.Lmicroseconds)}
}

// NewWriterLogger returns a Logger writing decision lines to w (nil -> stdout).
func NewWriterLogger(w io.Writer) Logger { return newLogger(w) }

func (s *stdLogger) Printf(format string, args ...any) {
	s.l.Output(2, fmt.Sprintf(format, args...))
}

// discardLogger drops all output.
type discardLogger struct{}

func (discardLogger) Printf(string, ...any) {}

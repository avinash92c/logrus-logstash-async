package logstash

import (
	"fmt"
	"io"
	"sync"

	"github.com/sirupsen/logrus"
)

var BufSize uint = 8192

type Hook struct {
	writer    io.Writer
	formatter logrus.Formatter
	buf       chan *logrus.Entry
	wg        sync.WaitGroup
	mu        sync.RWMutex
}

func New(w io.Writer, f logrus.Formatter) *Hook {
	hook := &Hook{
		writer:    w,
		formatter: f,
		buf:       make(chan *logrus.Entry, BufSize),
	}
	go hook.fire() // Log in background
	return hook
}

func (hook *Hook) fire() {
	for {
		e := <-hook.buf // receive new entry on channel

		dataBytes, err := hook.formatter.Format(e)
		if err != nil {
			// return err
			fmt.Println(err)
		}
		_, err = hook.writer.Write(dataBytes)
		if err != nil {
			// return err
			fmt.Println(err)
		}
		// return err

		hook.wg.Done()
	}
}

func (hook *Hook) Flush() {
	hook.mu.Lock() // claim the mutex as a Lock - we want exclusive access to it
	defer hook.mu.Unlock()

	hook.wg.Wait()
}

func (hook *Hook) Fire(e *logrus.Entry) error {
	hook.mu.RLock() // Claim the mutex as a RLock - allowing multiple go routines to log simultaneously
	defer hook.mu.RUnlock()

	hook.wg.Add(1)
	hook.buf <- e
	return nil
}

func (h *Hook) Levels() []logrus.Level {
	return logrus.AllLevels
}

var entryPool = sync.Pool{
	New: func() interface{} {
		return &logrus.Entry{}
	},
}

func copyEntry(e *logrus.Entry, fields logrus.Fields) *logrus.Entry {
	ne := entryPool.Get().(*logrus.Entry)
	ne.Message = e.Message
	ne.Level = e.Level
	ne.Time = e.Time
	ne.Data = logrus.Fields{}

	if e.HasCaller() {
		ne.Data["function"] = e.Caller.Function
		ne.Data["file"] = fmt.Sprintf("%s:%d", e.Caller.File, e.Caller.Line)
	}

	for k, v := range fields {
		ne.Data[k] = v
	}
	for k, v := range e.Data {
		ne.Data[k] = v
	}
	return ne
}

func releaseEntry(e *logrus.Entry) {
	entryPool.Put(e)
}

type LogstashFormatter struct {
	logrus.Formatter
	logrus.Fields
}

var (
	logstashFields   = logrus.Fields{"@version": "1", "type": "log"}
	logstashFieldMap = logrus.FieldMap{
		logrus.FieldKeyTime: "@timestamp",
		logrus.FieldKeyMsg:  "message",
	}
)

func DefaultFormatter(fields logrus.Fields) logrus.Formatter {
	for k, v := range logstashFields {
		if _, ok := fields[k]; !ok {
			fields[k] = v
		}
	}

	return LogstashFormatter{
		Formatter: &logrus.JSONFormatter{FieldMap: logstashFieldMap},
		Fields:    fields,
	}
}

func (f LogstashFormatter) Format(e *logrus.Entry) ([]byte, error) {
	ne := copyEntry(e, f.Fields)
	dataBytes, err := f.Formatter.Format(ne)
	releaseEntry(ne)
	return dataBytes, err
}

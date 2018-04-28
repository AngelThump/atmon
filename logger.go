package main

import (
	"bytes"
	"log"
	"time"
)

const (
	// LogSizeLimit bigquery request size limit is 10MB
	LogSizeLimit = 9 * 1014 * 1024

	// LogAgeLimit limit log staleness
	LogAgeLimit = 10 * time.Minute

	// ReportBufferSize maximum unprocessed reports
	ReportBufferSize = 128

	// RecordsPerAvroBlock ...
	RecordsPerAvroBlock = 100
)

// Logger ...
type Logger struct {
	writer  *BigQueryWriter
	reports chan *Report
	buffers chan *bytes.Buffer
}

// NewLogger ...
func NewLogger(writer *BigQueryWriter) *Logger {
	l := &Logger{
		writer:  writer,
		reports: make(chan *Report, ReportBufferSize),
		buffers: make(chan *bytes.Buffer, 1),
	}

	go l.doWriteLoop()
	go l.doBufferLoop()

	return l
}

func (l *Logger) doWriteLoop() {
	for {
		buf := <-l.buffers

		log.Printf("writing %d bytes to bigquery", buf.Len())

		if err := l.writer.Write(buf); err != nil {
			log.Println("error writing to bigquery", err)
		}
	}
}

func (l *Logger) doBufferLoop() {
	buf, writer := l.initBuffer()
	lastFlush := time.Now()
	for {
		report := <-l.reports
		writer.WriteReport(report)

		if buf.Len() > LogSizeLimit || (time.Since(lastFlush) > LogAgeLimit && buf.Len() > 0) {
			writer.Flush()
			l.buffers <- buf

			buf, writer = l.initBuffer()
			lastFlush = time.Now()
		}
	}
}

func (l *Logger) initBuffer() (*bytes.Buffer, *EventWriter) {
	buf := bytes.Buffer{}
	writer, err := NewEventWriter(&buf, RecordsPerAvroBlock)
	if err != nil {
		log.Fatal("error initializing buffer", err)
	}
	return &buf, writer
}

// WriteReport ...
func (l *Logger) WriteReport(report *Report) {
	log.Printf(
		"received %d events (Play: %d, Stalled: %d, Waiting: %d, Resource: %d)",
		len(report.Play)+len(report.Stalled)+len(report.Waiting)+len(report.Resource),
		len(report.Play),
		len(report.Stalled),
		len(report.Waiting),
		len(report.Resource),
	)

	l.reports <- report
}

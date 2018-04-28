package main

import (
	"io"
	"log"

	"github.com/alanctgardner/gogen-avro/container"
	"github.com/slugalisk/atmon/avro"
)

// EventWriter ...
type EventWriter struct {
	w *container.Writer
}

// NewEventWriter ...
func NewEventWriter(w io.Writer, recordsPerBlock int64) (*EventWriter, error) {
	ew, err := avro.NewEventWriter(w, container.Deflate, recordsPerBlock)
	if err != nil {
		return nil, err
	}

	return &EventWriter{ew}, nil
}

// Flush ...
func (l *EventWriter) Flush() error {
	return l.w.Flush()
}

func (l *EventWriter) writeBufferEvents(report *Report, events []BufferEvent) {
	for _, e := range events {
		err := l.w.WriteRecord(&avro.Event{
			Header: &e.Header,
			BufferTime: avro.UnionNullBufferTime{
				BufferTime: &e.BufferTime,
				UnionType:  avro.UnionNullBufferTimeTypeEnumBufferTime,
			},
			Network: report.Network,
			Geo:     report.Geo,
		})
		if err != nil {
			log.Println(err)
		}
	}
}

func (l *EventWriter) writeResourceEvents(report *Report, events []ResourceEvent) {
	for _, e := range events {
		err := l.w.WriteRecord(&avro.Event{
			Header: &e.Header,
			ResourceTime: avro.UnionNullResourceTime{
				ResourceTime: &e.ResourceTime,
				UnionType:    avro.UnionNullResourceTimeTypeEnumResourceTime,
			},
			Network: report.Network,
			Geo:     report.Geo,
		})
		if err != nil {
			log.Println(err)
		}
	}
}

// WriteReport ...
func (l *EventWriter) WriteReport(report *Report) {
	l.writeBufferEvents(report, report.Play)
	l.writeBufferEvents(report, report.Stalled)
	l.writeBufferEvents(report, report.Waiting)
	l.writeResourceEvents(report, report.Resource)
}

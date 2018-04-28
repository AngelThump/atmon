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
func NewEventWriter(w io.Writer) (*EventWriter, error) {
	ew, err := avro.NewEventWriter(w, container.Deflate, 100)
	if err != nil {
		return nil, err
	}

	return &EventWriter{ew}, nil
}

// Flush ...
func (l *EventWriter) Flush() error {
	return l.w.Flush()
}

func (l *EventWriter) writeBufferEvents(events []BufferEvent) {
	for _, e := range events {
		err := l.w.WriteRecord(&avro.Event{
			Header: &e.Header,
			BufferTime: avro.UnionNullBufferTime{
				BufferTime: &e.BufferTime,
				UnionType:  avro.UnionNullBufferTimeTypeEnumBufferTime,
			},
		})
		if err != nil {
			log.Println(err)
		}
	}
}

func (l *EventWriter) writeResourceEvents(events []ResourceEvent) {
	for _, e := range events {
		err := l.w.WriteRecord(&avro.Event{
			Header: &e.Header,
			ResourceTime: avro.UnionNullResourceTime{
				ResourceTime: &e.ResourceTime,
				UnionType:    avro.UnionNullResourceTimeTypeEnumResourceTime,
			},
		})
		if err != nil {
			log.Println(err)
		}
	}
}

// WriteClientReport ...
func (l *EventWriter) WriteClientReport(r *ClientReport) {
	l.writeBufferEvents(r.Play)
	l.writeBufferEvents(r.Stalled)
	l.writeBufferEvents(r.Waiting)
	l.writeResourceEvents(r.Resource)
}

package main

import "github.com/slugalisk/atmon/avro"

// BufferEvent ...
type BufferEvent struct {
	avro.Header
	avro.BufferTime
}

// ResourceEvent ...
type ResourceEvent struct {
	avro.Header
	avro.ResourceTime
}

// ClientReport ...
type ClientReport struct {
	Play     []BufferEvent
	Stalled  []BufferEvent
	Waiting  []BufferEvent
	Resource []ResourceEvent
}

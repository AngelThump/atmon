package main

import (
	"time"

	"github.com/slugalisk/atmon/avro"
)

// SecondsPerDay ...
const SecondsPerDay int64 = 86400

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

// Report ...
type Report struct {
	Date int32
	Time int64
	*ClientReport
	Network avro.UnionNullNetwork
	Geo     avro.UnionNullGeo
}

// NewReport ...
func NewReport() *Report {
	now := time.Now().Unix()

	return &Report{
		Date:         int32(now / SecondsPerDay),
		Time:         now,
		ClientReport: &ClientReport{},
	}
}

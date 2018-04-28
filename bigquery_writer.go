package main

import (
	"context"
	"flag"
	"io"
	"log"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/option"
)

// BigQueryWriterConfig ...
type BigQueryWriterConfig struct {
	ProjectID          string
	DatasetID          string
	TableID            string
	ServiceAccountJSON string
}

// InitFlags ...
func (c *BigQueryWriterConfig) InitFlags() {
	flag.StringVar(&c.ProjectID, "project-id", "", "google cloud platform project id")
	flag.StringVar(&c.DatasetID, "dataset-id", "", "id for dataset")
	flag.StringVar(&c.TableID, "table-id", "", "id for table")
	flag.StringVar(&c.ServiceAccountJSON, "service-account-json", "", "google service account json path")
}

// BigQueryWriter ...
type BigQueryWriter struct {
	client *bigquery.Client
	config BigQueryWriterConfig
}

// NewBigQueryWriter ..
func NewBigQueryWriter(config BigQueryWriterConfig) (*BigQueryWriter, error) {
	client, err := bigquery.NewClient(
		context.Background(),
		config.ProjectID,
		option.WithServiceAccountFile(config.ServiceAccountJSON),
	)
	if err != nil {
		return nil, err
	}

	return &BigQueryWriter{client, config}, nil
}

// Write ...
func (w *BigQueryWriter) Write(b io.Reader) error {
	source := bigquery.NewReaderSource(b)
	source.AllowJaggedRows = true
	source.SourceFormat = bigquery.Avro

	loader := w.client.Dataset(w.config.DatasetID).Table(w.config.TableID).LoaderFrom(source)
	loader.CreateDisposition = bigquery.CreateIfNeeded
	loader.WriteDisposition = bigquery.WriteAppend

	job, err := loader.Run(context.Background())
	if err != nil {
		return err
	}
	status, err := job.Wait(context.Background())
	if err != nil {
		return err
	}
	if err := status.Err(); err != nil {
		return err
	}

	stats := status.Statistics.Details.(*bigquery.LoadStatistics)
	log.Printf(
		"finished loading data intto bigquery (TotalBytesProcessed: %d, InputFileBytes: %d, OutputBytes: %d, OutputRows: %d)",
		status.Statistics.TotalBytesProcessed,
		stats.InputFileBytes,
		stats.OutputBytes,
		stats.OutputRows,
	)
	return nil
}

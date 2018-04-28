package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
)

var bigQueryWriterConfig BigQueryWriterConfig
var httpAddress string

func init() {
	bigQueryWriterConfig.InitFlags()
	flag.StringVar(&httpAddress, "http-addr", ":80", "http server address")
}

func main() {
	flag.Parse()

	bigQueryWriter, err := NewBigQueryWriter(&bigQueryWriterConfig)
	if err != nil {
		log.Fatal("error starting bq client", err)
	}

	logger := NewLogger(bigQueryWriter)

	handleReport := func(res http.ResponseWriter, req *http.Request) {
		res.Header().Add("Content-Type", "application/json")

		if req.Method != http.MethodPost {
			http.Error(res, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		var report ClientReport
		err := json.NewDecoder(req.Body).Decode(&report)
		if err != nil {
			http.Error(res, `{"error":"bad request"}`, http.StatusBadRequest)
			return
		}
		defer req.Body.Close()

		logger.WriteClientReport(&report)

		res.WriteHeader(http.StatusOK)
		res.Write([]byte(`{}`))
	}

	router := mux.NewRouter()
	router.HandleFunc("/api/v1/report", handleReport).Methods("POST")

	corsMiddleware := handlers.CORS(handlers.AllowedOrigins([]string{"*"}))

	log.Fatal(http.ListenAndServe(httpAddress, corsMiddleware(router)))
}

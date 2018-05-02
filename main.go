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
var geoIPDBConfig GeoIPDBConfig
var httpAddress string
var ipHeader string

func init() {
	bigQueryWriterConfig.InitFlags()
	geoIPDBConfig.InitFlags()
	flag.StringVar(&httpAddress, "http-addr", ":80", "http server address")
	flag.StringVar(&ipHeader, "ip-header", "X-Client-IP", "http header with client ip")
}

func main() {
	flag.Parse()

	bigQueryWriter, err := NewBigQueryWriter(bigQueryWriterConfig)
	if err != nil {
		log.Fatal("error starting bq client", err)
	}

	geoIPDB, err := NewGeoIPDB(geoIPDBConfig)
	if err != nil {
		log.Fatal("error initializing geoip db", err)
	}

	logger := NewLogger(bigQueryWriter)

	handleReport := func(res http.ResponseWriter, req *http.Request) {
		res.Header().Add("Content-Type", "application/json")

		report := NewReport()
		err := json.NewDecoder(req.Body).Decode(report.ClientReport)
		if err != nil {
			http.Error(res, `{"error":"bad request"}`, http.StatusBadRequest)
			return
		}
		defer req.Body.Close()

		if ip := req.Header.Get(ipHeader); ip != "" {
			if network, city, err := geoIPDB.Lookup(ip); err == nil {
				report.Network = network.AvroNetwork()
				report.Geo = city.AvroGeo()
			}
		}

		logger.WriteReport(report)

		res.WriteHeader(http.StatusOK)
		res.Write([]byte(`{}`))
	}

	router := mux.NewRouter()
	router.HandleFunc("/api/v1/report", handleReport).Methods("POST")

	corsHeaders := handlers.AllowedHeaders([]string{"Content-Type", "Origin"})
	corsMaxAge := handlers.MaxAge(86400)
	corsOrigins := handlers.AllowedOrigins([]string{"*"})
	corsMethods := handlers.AllowedMethods([]string{"GET", "OPTIONS"})
	corsMiddleware := handlers.CORS(corsHeaders, corsMaxAge, corsOrigins, corsMethods)

	log.Fatal(http.ListenAndServe(httpAddress, corsMiddleware(router)))
}

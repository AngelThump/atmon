package main

import (
	"flag"
	"log"
	"net"

	"github.com/oschwald/maxminddb-golang"
	"github.com/slugalisk/atmon/avro"
)

// GeoIPDBConfig ...
type GeoIPDBConfig struct {
	ASNsPath   string
	CitiesPath string
}

// InitFlags ...
func (g *GeoIPDBConfig) InitFlags() {
	flag.StringVar(&g.ASNsPath, "geoip-asns-path", "", "geolite2 ASNs db path")
	flag.StringVar(&g.CitiesPath, "geoip-cities-path", "", "geolite2 cities db path")
}

// GeoIPDB ...
type GeoIPDB struct {
	asns   *maxminddb.Reader
	cities *maxminddb.Reader
}

// NewGeoIPDB ...
func NewGeoIPDB(config GeoIPDBConfig) (*GeoIPDB, error) {
	asns, err := maxminddb.Open(config.ASNsPath)
	if err != nil {
		return nil, err
	}

	cities, err := maxminddb.Open(config.CitiesPath)
	if err != nil {
		return nil, err
	}

	return &GeoIPDB{asns, cities}, nil
}

// Close ...
func (g *GeoIPDB) Close() {
	g.cities.Close()
	g.asns.Close()
}

// Lookup ...
func (g *GeoIPDB) Lookup(ip string) (*ASNRecord, *CityRecord, error) {
	var asn ASNRecord
	var city CityRecord

	netIP := net.ParseIP(ip)
	if err := g.asns.Lookup(netIP, &asn); err != nil {
		log.Println("error looking up asn for ip", err)
	}
	if err := g.cities.Lookup(netIP, &city); err != nil {
		log.Println("error looking up city for ip", err)
	}

	return &asn, &city, nil
}

// ASNRecord ...
type ASNRecord struct {
	ASN          int32  `maxminddb:"autonomous_system_number"`
	Organization string `maxminddb:"autonomous_system_organization"`
}

// AvroNetwork ...
func (r *ASNRecord) AvroNetwork() avro.UnionNullNetwork {
	if r.ASN == 0 {
		return avro.UnionNullNetwork{}
	}

	return avro.UnionNullNetwork{
		Network: &avro.Network{
			ASN:          r.ASN,
			Organization: r.Organization,
		},
		UnionType: avro.UnionNullNetworkTypeEnumNetwork,
	}
}

// CityRecord ...
type CityRecord struct {
	Subdivisions []struct {
		GeoNameID int32  `maxminddb:"geoname_id"`
		ISOCode   string `maxminddb:"iso_code"`
		Names     struct {
			Name string `maxminddb:"en"`
		} `maxminddb:"names"`
	} `maxminddb:"subdivisions"`
	City struct {
		GeoNameID int32 `maxminddb:"geoname_id"`
		Names     struct {
			Name string `maxminddb:"en"`
		} `maxminddb:"names"`
	} `maxminddb:"city"`
	Continent struct {
		GeoNameID int32  `maxminddb:"geoname_id"`
		Code      string `maxminddb:"code"`
		Names     struct {
			Name string `maxminddb:"en"`
		} `maxminddb:"names"`
	} `maxminddb:"continent"`
	Country struct {
		GeoNameID int32  `maxminddb:"geoname_id"`
		ISOCode   string `maxminddb:"iso_code"`
		Names     struct {
			Name string `maxminddb:"en"`
		} `maxminddb:"names"`
		IsInEuropeanUnion bool `maxminddb:"is_in_european_union"`
	} `maxminddb:"country"`
	Traits struct {
		IsAnonymousProxy    bool `maxminddb:"is_anonymous_proxy"`
		IsSatelliteProvider bool `maxminddb:"is_satellite_provider"`
	} `maxminddb:"traits"`
}

// AvroGeo ...
func (r *CityRecord) AvroGeo() avro.UnionNullGeo {
	if r.Country.GeoNameID == 0 {
		return avro.UnionNullGeo{}
	}

	geo := &avro.Geo{
		CityID:              r.City.GeoNameID,
		CityName:            r.City.Names.Name,
		ContinentID:         r.Continent.GeoNameID,
		ContinentCode:       r.Continent.Code,
		ContinentName:       r.Continent.Names.Name,
		CountryID:           r.Country.GeoNameID,
		CountryISO:          r.Country.ISOCode,
		CountryName:         r.Country.Names.Name,
		IsInEuropeanUnion:   r.Country.IsInEuropeanUnion,
		IsAnonymousProxy:    r.Traits.IsAnonymousProxy,
		IsSatelliteProvider: r.Traits.IsSatelliteProvider,
	}

	if len(r.Subdivisions) != 0 {
		geo.SubdivisionID = r.Subdivisions[0].GeoNameID
		geo.SubdivisionISO = r.Subdivisions[0].ISOCode
		geo.SubdivisionName = r.Subdivisions[0].Names.Name
	}

	return avro.UnionNullGeo{
		Geo:       geo,
		UnionType: avro.UnionNullGeoTypeEnumGeo,
	}
}

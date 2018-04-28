update-geolite2:
	mkdir -p build/temp; \
	cd build/temp; \
	wget http://geolite.maxmind.com/download/geoip/database/GeoLite2-City.tar.gz; \
	wget http://geolite.maxmind.com/download/geoip/database/GeoLite2-ASN.tar.gz; \
	tar xzf GeoLite2-City.tar.gz; \
	tar xzf GeoLite2-ASN.tar.gz; \
	mv */*.mmdb ../; \
	cd ..; \
	rm -rf temp

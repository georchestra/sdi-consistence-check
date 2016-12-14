## About

This directory allows the creation of a docker image (tagged gngs) to make the
development on this project easier.

Simply run the provided `build.sh` script, then launch:

```
$ docker run -p8080:8080 gngs
```

You should now have a `geoserver 2.10` and a `geonetwork 3.2` available at:

* http://localhost:8080/geonetwork/
* http://localhost:8080/geoserver/

respectively.

Next, it is advised to connect onto the GeoNetwork webapp (`admin`/`admin`),
configure a WxS harvester onto the GeoServer, activate the edition of the
harvested MDs, then go back to the GeoServer to edit several layers and add a
link back to the GeoNetwork.



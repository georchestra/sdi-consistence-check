# SDI consistence check

## Configure

If you need to check against protected services, you
can create a configuration file in your home directory `~/.sdichecker` with following format :

```
<hostname> <username> <password>
```

Example :
```
sdi.georchestra.org testadmin testadmin
```


## Run
```
usage: checker.py [-h] [--mode {WMS,WFS,CSW}] [--inspire {flexible,strict}]
                  [--server SERVER]
                  [--geoserver-to-check GEOSERVER_TO_CHECK [GEOSERVER_TO_CHECK ...]]

optional arguments:
  -h, --help            show this help message and exit
  --mode {WMS,WFS,CSW}  the mode to consider (WMS, WFS, CSW)
  --inspire {flexible,strict}
                        indicates if the checks should be strict or flexible,
                        default to flexible
  --server SERVER       the server to target (full URL, e.g.
                        https://sdi.georchestra.org/geoserver/wms)
  --geoserver-to-check GEOSERVER_TO_CHECK [GEOSERVER_TO_CHECK ...]
                        space-separated list of geoserver hostname to check in
                        CSW mode. Ex: sdi.georchestra.org
```

You need to choose one "mode" from :

 * WMS : check consistency of MD found in getCapabilities response from WMS service
 * WFS : same as previous for WFS service
 * CSW : check availability of WMS or WFS service for MD found in CSW service.
    For this mode, you can choose from flexible or strict inspire compliance.

Examples :

Check consistency from a geoserver instance to a geonetwork using WMS :
```
python3 checker.py --mode WMS --server https://sdi.georchestra.org/geoserver/wms
```
Same using WFS service :
```
python3 checker.py --mode WFS --server https://sdi.georchestra.org/geoserver/wfs
```
Check CSW service :
```
python3 checker.py --mode CSW --inspire=strict --geoserver-to-check sdi.georchestra.org --server https://sdi.georchestra.org/geonetwork/srv/fre/csw
```


## Build docker image
```
git clone https://github.com/sigrennesmetropole/sdi-consistence-check.git
cd sdi-consistence-check && docker build -t georchestra/sdi-consistence-check .
```


## Run with docker
```
docker run -e WMS_SERVICE='https://sdi.georchestra.org/geoserver/wms' -it --rm georchestra/sdi-consistence-check
```

Work sponsored by [Service de l'Information Géographique de Rennes Métropole](https://github.com/sigrennesmetropole/)

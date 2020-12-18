# SDI consistence check

[![Pulls](https://img.shields.io/docker/pulls/georchestra/sdi-consistence-check.svg)](https://hub.docker.com/r/georchestra/sdi-consistence-check/)

This project aims to check the relevance between data (published into a GeoServer) and metadata (in GeoNetwork), and possibly fix the detected inconsistencies when possible (missing metadata, missing URL information, ...), following different scenarios.

## Usage

```
usage: checker.py [-h] --mode {WMS,WFS,CSW} [--inspire {flexible,strict}]
                  [--server SERVER]
                  [--geoserver-to-check GEOSERVER_TO_CHECK [GEOSERVER_TO_CHECK ...]]
                  [--disable-ssl-verification] [--only-err] [--xunit] [--check-layers]
                  [--xunit-output XUNIT_OUTPUT] [--log-to-file LOG_TO_FILE]

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
                        CSW mode with inspire strict option activated. Ex:
                        sdi.georchestra.org
  --check-layers        check WMS/WFS layer validity by performing sample WMS 
                        GetMap or WFS GetFeature requests
  --disable-ssl-verification
                        Disable certificate verification
  --only-err            Only display errors, no summary informations will be
                        displayed
  --xunit               Generate a XML xunit result report
  --xunit-output XUNIT_OUTPUT
                        Name of the xunit report file, defaults to ./xunit.xml
  --log-to-file LOG_TO_FILE
                        If a file path is specified, log output to this file,
                        not stdout
```

You need to choose one "mode" from :

 * WMS : check consistency of MD found in getCapabilities response from WMS service
 * WFS : same as previous for WFS service
 * CSW : check availability of WMS or WFS service for MD found in CSW service.
    For this mode, you can choose from flexible or strict inspire compliance.


Example usage with a public WMS service:
 * with docker:
```
docker run --rm georchestra/sdi-consistence-check --mode WMS --server https://www.geopicardie.fr/geoserver/wms
```
 * without docker (requires installation, see below):
```
python3 checker.py --mode WMS --server https://www.geopicardie.fr/geoserver/wms
```


In case a private service is to be checked, you should first create a `.sdichecker` in your home directory, with the following format:
```
<hostname> <username> <password>
```
Example :
```
sdi.georchestra.org testadmin testadmin
```

Example usage with a private service, requiring credentials.
 * with docker:
```
docker run --rm -v $HOME/.sdichecker:/.sdichecker --user=`id -u $USER` georchestra/sdi-consistence-check \
 --mode WMS --server https://www.geopicardie.fr/geoserver/wms
```
 * without docker (requires installation, see below):
```
python3 checker.py --mode WMS --server https://www.geopicardie.fr/geoserver/wms
```

### Advanced Usage


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
python3 checker.py --mode CSW --inspire=strict --geoserver-to-check sdi.georchestra.org \
  --server https://sdi.georchestra.org/geonetwork/srv/fre/csw
```

### Xunit format

Xunit is an XML report output format used by several test frameworks, as Junit.
using the options `--xunit` / `--xunit-output` will provide a report in this
format, convenient if plugged in a CI environment like Jenkins.

## Setup

### Classic setup using a virtualenv

To install the needed dependencies, follow these steps. Note that a python3 runtime is required to launch this tool.

```
# Make sure the needed dependencies are installed
apt-get install virtualenv gcc python3-dev libproj-dev

# clone the repository
git clone https://github.com/georchestra/sdi-consistency-check.git
cd sdi-consistency-check

virtualenv -p python3 venv
source venv/bin/activate

pip install -r requirements.txt
```

### Building the docker image

```
git clone https://github.com/sigrennesmetropole/sdi-consistence-check.git
cd sdi-consistence-check && docker build -t georchestra/sdi-consistence-check .
```

## Testing

Nose is used for unit testing ; to launch the testsuite, just use the following command:

```
nosetests
```

## About / Acknowledgements

Work sponsored by [Service de l'Information Géographique de Rennes Métropole](https://github.com/sigrennesmetropole/)

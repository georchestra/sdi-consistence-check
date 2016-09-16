# SDI consistence check

Build:
```
git clone https://github.com/sigrennesmetropole/sdi-consistence-check.git
cd sdi-consistence-check && docker build -t georchestra/sdi-consistence-check .
```

Run:
```
docker run -e WMS_SERVICE='https://sdi.georchestra.org/geoserver/wms' -it --rm georchestra/sdi-consistence-check
```
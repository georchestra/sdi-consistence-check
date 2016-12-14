
if [ ! -f geoserver-2.10.0-war.zip ] ; then
wget http://sourceforge.net/projects/geoserver/files/GeoServer/2.10.0/geoserver-2.10.0-war.zip
unzip geoserver-2.10.0-war.zip
fi

if [ ! -f geonetwork.war ] ; then
wget -O geonetwork.war 'http://downloads.sourceforge.net/project/geonetwork/GeoNetwork_opensource/v3.2.0/geonetwork.war?r=https%3A%2F%2Fsourceforge.net%2Fprojects%2Fgeonetwork%2Ffiles%2FGeoNetwork_opensource%2Fv3.2.0%2F&ts=1481706382&use_mirror=heanet'
fi

docker build -t gngs .

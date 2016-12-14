from jetty:jre8

COPY geonetwork.war /var/lib/jetty/webapps
COPY geoserver.war /var/lib/jetty/webapps

ENV JAVA_OPTS "-Xmx4g"

EXPOSE 8080

docker-build:
	TAG=$$(date +%Y%m%d%H%M%S)_$$(git rev-parse --short HEAD) ;\
	docker pull python:3.5 ; \
	docker build -t georchestra/sdi-consistence-check:$$TAG . ; \
	docker build -t georchestra/sdi-consistence-check:latest . ; \

all: docker-build

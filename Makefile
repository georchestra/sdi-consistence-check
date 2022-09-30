TAG=$(shell git rev-parse --short HEAD)

docker-build:
	docker pull python:3.5
	docker build -t georchestra/sdi-consistence-check:$(TAG) .
	docker build -t georchestra/sdi-consistence-check:latest .

all: docker-build

clean:
	rm -rf venv
	docker rmi georchestra/sdi-consistence-check:$(TAG) || true
	docker rmi georchestra/sdi-consistence-check:latest || true

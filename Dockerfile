FROM python:3.5

MAINTAINER PSC "psc@georchestra.org"

RUN groupadd --gid 999 snake && \
    useradd -r -ms /bin/bash -d /home/snake --uid 999 --gid 999 snake

RUN pip install owslib gsconfig-py3 mako==1.0.6 pydevd

COPY . /app
WORKDIR /app

USER snake

ENTRYPOINT ["python", "checker.py"]

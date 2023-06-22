FROM python:3.11

MAINTAINER PSC "psc@georchestra.org"

RUN groupadd --gid 999 snake && \
    useradd -r -ms /bin/bash -d /home/snake --uid 999 --gid 999 snake

COPY requirements.txt /requirements.txt
RUN pip install -r /requirements.txt \
    && rm -rf /root/.cache

COPY ./sdi-consistence-check /app
WORKDIR /app

USER snake

ENTRYPOINT ["python", "checker.py"]

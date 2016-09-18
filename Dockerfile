FROM python:3.5

MAINTAINER PSC "psc@georchestra.org"

COPY checker.py .
COPY requirements.txt /tmp

RUN pip install -r /tmp/requirements.txt

RUN groupadd --gid 999 snake && \
    useradd -r -ms /bin/bash --uid 999 --gid 999 snake

USER snake

CMD ["python", "checker.py"]
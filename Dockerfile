FROM python:3.5

MAINTAINER PSC "psc@georchestra.org"

COPY checker.py .
COPY requirements.txt /tmp

RUN pip install -r /tmp/requirements.txt

CMD ["python", "checker.py"]
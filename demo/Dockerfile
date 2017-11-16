# docker image to run the arq demo
# image must be built using build.sh
FROM python:3.6-alpine

LABEL maintainer "s@muelcolvin.com"

RUN apk --update --no-cache add gcc g++ musl-dev libuv make \
 && rm -rf /var/cache/apk/*

RUN pip install -U pip setuptools
ADD ./docker-requirements.txt /home/root/
RUN pip install -r /home/root/docker-requirements.txt

ADD ./setup.py /home/root/src/
ADD ./arq /home/root/src/arq
RUN pip install -e /home/root/src/
ADD ./app.py /home/root/

ENV PYTHONUNBUFFERED 1
WORKDIR /home/root
ENTRYPOINT ["./app.py"]

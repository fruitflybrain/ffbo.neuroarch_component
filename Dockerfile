# docker build -t fruitflybrain/ffbo.neuroarch_component:hemibrain .

# Initialize image
FROM python:2
MAINTAINER Yiyin Zhou <yiyin@ee.columbia.edu>
RUN apt-get update && apt-get install -y apt-transport-https

ENV HOME /app
ENV DEBIAN_FRONTEND noninteractive

# Add the application resources URL
RUN echo "deb http://archive.ubuntu.com/ubuntu/ trusty main universe" >> /etc/apt/sources.list
RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 40976EAF437D05B5
RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 3B4FE6ACC0B21F32
RUN apt-get update

# Install basic applications
RUN apt-get install -y --force-yes tar git curl vim wget dialog net-tools build-essential

# Install Python and Basic Python Tools
RUN apt-get install -y --force-yes --force-yes python python-dev python-distribute python-pip

# Crossbar.io connection defaults
ENV CBURL ws://crossbar:8080/ws
ENV CBREALM realm1

# install Autobahn|Python
RUN pip install -U pip && pip install autobahn[twisted]==18.12.1

# Install Java
RUN apt-get install -y --force-yes default-jre

# Install OrientDB
RUN wget https://s3.us-east-2.amazonaws.com/orientdb3/releases/2.2.37/orientdb-community-importers-2.2.37.zip && \
    unzip orientdb-community-importers-2.2.37.zip -d /opt && \
    mv /opt/orientdb-community-importers-2.2.37 /opt/orientdb && \
    rm orientdb-community-importers-2.2.37.zip
RUN sed -e "s/-d64 //g" -i.backup /opt/orientdb/bin/server.sh

# Install dependancies
RUN pip install --upgrade pip
RUN pip install numpy==1.14.5 cython simplejson daff path.py networkx==1.11


RUN  apt-get -yq update && \
     apt-get -yqq install ssh
RUN apt-get install -y --force-yes python-h5py
RUN apt-get clean
RUN rm -r /var/lib/apt/lists/*
RUN apt-get update

# Package that supports binary serialization for pyorient
RUN pip install pyorient_native pyOpenSSL pandas service_identity configparser

# Install from forked pyorient till binary serialization support
# is integrated in the next release
#WORKDIR /app
#RUN git clone https://github.com/nikulukani/pyorient.git
#WORKDIR /app/pyorient
#RUN git fetch && git checkout develop
#RUN python setup.py install

RUN pip install pyorient

ENV ORIENTDB_ROOT_PASSWORD root

RUN git clone --single-branch -b hemibrain_vnc https://github.com/fruitflybrain/ffbo.neuroarch_component /neuroarch_component
RUN git clone https://github.com/fruitflybrain/neuroarch /neuroarch

# You can optionally download and install the database when building the image,
# otherwise download the database dump using the link below or wget command further below
# and map volume into the docker container.
# https://drive.google.com/file/d/1lWCQPw5A6-HwH5oFsGFKDHw7S6JEsvqY/view?usp=sharing

# Install database
#WORKDIR /opt/orientdb/databases
#RUN wget --load-cookies /tmp/cookies.txt "https://docs.google.com/uc?export=download&confirm=$(wget --quiet --save-cookies /tmp/cookies.txt --keep-session-cookies --no-check-certificate 'https://docs.google.com/uc?export=download&id=1lWCQPw5A6-HwH5oFsGFKDHw7S6JEsvqY' -O- | sed -rn 's/.*confirm=([0-9A-Za-z_]+).*/\1\n/p')&id=1lWCQPw5A6-HwH5oFsGFKDHw7S6JEsvqY" -O ffbo_db.tar.gz && rm -rf /tmp/cookies.txt && \
#    tar zxvf ffbo_db.tar.gz && \
#    rm ffbo_db.tar.gz

WORKDIR /neuroarch_component/neuroarch_component

CMD sh run_component_docker.sh

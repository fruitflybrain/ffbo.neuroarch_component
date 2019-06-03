# Initialize image
FROM python:2
MAINTAINER Jonathan Marty <jonathan.n.marty@gmail.com>
RUN apt-get update && apt-get install -y apt-transport-https

ENV HOME /app
ENV DEBIAN_FRONTEND noninteractive

# Add the application resources URL
RUN echo "deb http://archive.ubuntu.com/ubuntu/ trusty main universe" >> /etc/apt/sources.list
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
RUN wget https://orientdb.com/download.php?file=orientdb-community-2.2.32.tar.gz
RUN tar -xf download.php?file=orientdb-community-2.2.32.tar.gz -C /opt
RUN mv /opt/orientdb-community-2.2.32 /opt/orientdb

# Install basic applications
RUN apt-get install -y --force-yes tar git curl vim wget dialog net-tools build-essential 


# Install dependancies
RUN pip install --upgrade pip
RUN pip install numpy==1.14.5
RUN pip install cython
RUN pip install simplejson

RUN pip install daff path.py
RUN pip install 'networkx==1.11'


RUN  apt-get -yq update && \
     apt-get -yqq install ssh
RUN apt-get install -y --force-yes python-h5py
RUN apt-get clean
RUN rm -r /var/lib/apt/lists/*
RUN apt-get update

# Install database
WORKDIR /opt/orientdb/databases
RUN wget -O ffbo_db.tar.gz https://www.dropbox.com/s/mjcs38m2we4uulr/ffbo_db_public.tar.gz?dl=0
RUN tar zxvf ffbo_db.tar.gz
WORKDIR /

# Package that supports binary serialization for pyorient
RUN pip install pyorient_native
RUN pip install pyOpenSSL
RUN pip install pandas
RUN pip install service_identity
RUN pip install configparser

# Install from forked pyorient till binary serialization support
# is integrated in the next release
#WORKDIR /app
#RUN git clone https://github.com/nikulukani/pyorient.git
#WORKDIR /app/pyorient
#RUN git fetch && git checkout develop
#RUN python setup.py install

RUN pip install pyorient

ENV ORIENTDB_ROOT_PASSWORD root

ADD . /neuroarch_component
RUN git clone https://github.com/fruitflybrain/neuroarch /neuroarch

WORKDIR /neuroarch_component/neuroarch_component

CMD sh run_component_docker.sh

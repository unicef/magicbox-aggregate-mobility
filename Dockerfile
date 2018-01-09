FROM ubuntu:16.04

WORKDIR /app

# Install project dependencies

# Install Java
RUN apt-get update -y && apt-get install -y wget curl gzip tar default-jdk

# Install Hadoop
RUN cd /tmp \
    && wget http://ftp.unicamp.br/pub/apache/hadoop/common/hadoop-3.0.0/hadoop-3.0.0.tar.gz \
    && tar -xzvf hadoop-3.0.0.tar.gz \
    && mv hadoop-3.0.0 /usr/local/hadoop 

ENV JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64/jre/"

ENV PATH="$PATH:/usr/local/hadoop/bin/"

# Install Spark
RUN cd /tmp \
    && wget http://ftp.unicamp.br/pub/apache/spark/spark-2.2.1/spark-2.2.1-bin-hadoop2.7.tgz \
    && tar -xzvf spark-2.2.1-bin-hadoop2.7.tgz \
    && mv spark-2.2.1-bin-hadoop2.7 /usr/local/spark

ENV PATH="$PATH:/usr/local/spark/bin/"

# nvm environment variables
ENV NVM_DIR="/usr/local/nvm"
ENV NODE_VERSION="7.10.1"

# Install NVM and Nodejs
RUN curl --silent -o- https://raw.githubusercontent.com/creationix/nvm/v0.33.8/install.sh | bash

# install node and npm
RUN \. $NVM_DIR/nvm.sh \
    && nvm install $NODE_VERSION \
    && nvm alias default $NODE_VERSION \
    && nvm use default

# add node and npm to path so the commands are available
ENV NODE_PATH="$NVM_DIR/v$NODE_VERSION/lib/node_modules"
ENV PATH="$NVM_DIR/versions/node/v$NODE_VERSION/bin:$PATH"

# Node dependencies
COPY package*.json ./

RUN npm install

ADD config_sample.js config.js

# Copy project files

COPY . .


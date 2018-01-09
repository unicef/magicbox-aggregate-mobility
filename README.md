# Aggregate Airport Mobility

This repository aggregates Amadeus mobility data by administrative boundary level 0 and outputs a csv file
where each line is `orig,dest,count`.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites

####NVM
```
curl -o- https://raw.githubusercontent.com/creationix/nvm/v0.33.8/install.sh | bash
```

####Node.js
```
nvm install 7
```

####Gunzip
```
sudo apt-get install gzip
```

####Apache Spark

Java
```
apt-get install default-jdk
```

Hadoop
```
wget http://ftp.unicamp.br/pub/apache/hadoop/common/hadoop-3.0.0/hadoop-3.0.0.tar.gz && tar -xzvf hadoop-3.0.0.tar.gz 
```

Make sure to `JAVA_HOME` available on your environment and `hadoop-3.0.0/bin` on your PATH


Spark
```
wget http://ftp.unicamp.br/pub/apache/spark/spark-2.2.1/spark-2.2.1-bin-hadoop2.7.tgz && tar -xzvf spark-2.2.1-bin-hadoop2.7.tgz
```

Make sure to also add `spark-2.2.1-bin-hadoop2.7/bin` to your PATH


### Installing

```
git clone https://github.com/unicef/aggregate_airport_mobility.git
cd aggregate_airport_mobility
cp config-sample.js config.js
npm install
```

## Usage

```
node main.js
```

## Running the tests

```
npm run test
```

##Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/unicef/aggregate_airport_mobility

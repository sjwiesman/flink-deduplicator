# Flink Deduplicator

A simple Flink application that reads from multiple kafka topics with the same schema and deduplicates them based on an id.

## Command Line Arguments
| Flag | Description |
|------| ------------ |
| topics | A comma separated lists of topics |
| bootstrap.servers | The kafka broker address|

## Usage

```bash
$ mvn package
$ /bin/flink run flink-dedeuplicator.jar --topics topic1,topic2 --boostrap.servers localhost:9000 
```
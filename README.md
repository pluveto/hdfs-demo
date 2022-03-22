# hdfs-demo

This is a demo project for BUPT big data course homework.

## Get started

(1) Clone this project

(2) Package it via:

```
mvn package
```

(3) Configure environment variable

| Name      | Description       | Example                         |
|-----------|-------------------|---------------------------------|
| HDFS_HOST | Host name of hdfs | hdfs://ecs-2019211379-0001:8020 |

(4) Run

> This need JDK 17

For example

```
C:\Users\i\.jdks\openjdk-17.0.2\bin\java.exe -jar .\target\hdfs-demo-1.0-SNAPSHOT-jar-with-dependencies.jar -h
```

## CLI Parameters

| Param              | Description                 | Is Required |
|--------------------|-----------------------------|-------------|
| `-a` `--action     | Action to perform           | true        |
| `-f` `--file       | Local path of file          | -           |
| `-t` `--target     | Remote path of file         | -           |
| `-d` `--target-dir | Remote dir                  | -           |
| `-l` `--length     | Length to read. default 1KB | -           |

### Actions

+ upload
+ download
+ list
+ head
+ tail

### Examples

List Directory

```shell
hdfs-demo --action list --target-dir /
```

Upload file

```shell
hdfs-demo --action upload --target /hello.txt --file hello.txt
```

Head file 10kb

```shell
hdfs-demo --action head --target /hello.txt --length 10240
```

Tail file 10kb

```shell
hdfs-demo --action tail --target /hello.txt --length 10240
```

Download file

```shell
hdfs-demo --action download --target /hello.txt --file downloaded.txt
```

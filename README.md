
mapreduce をscala で書いてみたサンプル

Mapper が読み込むデータは [NOAA の気象データ](ftp://ftp.ncdc.noaa.gov/pub/data/noaa/).


# 準備

sbt のプラグインとして [assembly](https://github.com/sbt/sbt-assembly) プラグインを利用する想定。
Hadoop は以下を利用

* CDH 5.13.0


# build

```
$ sbt assembly
```


# exec

```
$ sudo -u hdfs hadoop jar import-assembly-1.0.jar /path/to/inputdata /path/to/outputdir OUTPUT_FILE_PREFIX
```


# その他

* TODO: hbase 入出力のサンプル


# 参考

* [Hadoop MapReduceアプリの勉強](https://qiita.com/khiraiwa/items/8ac019d11d93f5041f2d)


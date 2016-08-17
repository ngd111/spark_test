# Written by Jin Kak Jung

Step 1
; import csv data to MongoDB

mongoimport mstf.csv --type csv --headerline -d marketdata -c minibars

Step 2
; Install MongoDB Hadoop Connector - PySpark
; Reference => https://spark.apache.org/docs/0.9.0/python-programming-guide.html

Step 3
; Install MongoDB Connector for Hadoop
; Reference => https://github.com/mongodb/mongo-hadoop
;           => https://github.com/mongodb/mongo-hadoop/tree/master/spark/src/main/python # Use pymongo_spark

Step 4
; Check existence of 3 jars files
; mongo-hadoop-core.jar mongo-hadoop-spark.jar mongo-java-driver.jar

Step 5
; Starting a cluster manually
; $SPARK_HOME/sbin/start-master.sh

Step 6
; Run pyspark
; pyspark --jars /usr/local/spark/lib/mongo-hadoop-core.jar,/usr/local/spark/lib/mongo-hadoop-spark.jar,/usr/local/spark/lib/mongo-java-driver.jar stock_analysis.py

Step 7
; Check mongoDB collection
; db.minbars_result.find()

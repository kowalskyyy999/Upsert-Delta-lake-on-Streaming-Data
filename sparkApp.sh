#!/bin/bash

$SPARK_HOME/bin/spark-submit \
	--master spark://kowalskyyy:7077 \
	--deploy-mode client \
	--packages io.delta:delta-core_2.12:2.0.0,org.postgresql:postgresql:42.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.2 \
	--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
    	--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
	--conf "spark.executor.memory=4g" \
	$1

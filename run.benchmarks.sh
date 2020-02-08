#!/usr/bin/env bash

mvn clean package -DskipTests
mvn dependency:copy-dependencies -DoutputDirectory=target/deps

#mvn package -DskipTests

java -cp "target/classes:target/deps/*" -Xmx6g -Xms6g com.milaboratory.benchmarks.milib.SerializationBenchmark1 -x 3

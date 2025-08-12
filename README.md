# ✨ Spark Build

This directory contains a custom `Dockerfile` and required libraries to build a specific version of Apache Spark.

## What's Inside?
Dockerfile: A custom script to build an image for Spark 3.3.4.

/conf/: A folder holding the configuration files that control how Spark runs.

spark-defaults.conf: Sets default properties for all jobs, like memory and CPU settings.

spark-env.sh: Sets environment variables for Spark, like the path to JAVA_HOME.

log4j2.properties: Controls the logging level to reduce unnecessary messages.

/jars/: A folder to hold any additional JARs (like database drivers) needed by Spark.

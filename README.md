# ✨ Spark Build & Configuration
This directory contains everything needed to build a custom Apache Spark Docker image and configure its properties.

## What's Inside?
* `Dockerfile`: A custom script to build an image for **Spark 3.3.4**.
* **/conf/ folder**: Holds the configuration files that control how Spark runs.
    * `spark-defaults.conf`: Sets default properties for all jobs, like memory and CPU settings.
    * `spark-env.sh`: Sets environment variables for Spark, like the path to **JAVA_HOME**.
    * `log4j2.properties`: Controls the logging level to reduce unnecessary messages.
* **/jars/ folder**: A folder to hold any additional JARs (like database drivers) needed by Spark.

Overview
-----------
This is a data pipeline that periodically fetches random user data from an API, transforms it, and streams it to a Kafka topic, before loading it into cassandra using spark.

Built With
-------------
1. Airflow
2. Apache kafka
3. Apache Cassandra
4. Apache Spark
5. Astro CLI


Setup the Project Locally
----------------------------
You need to have spark installed locally. Check https://spark.apache.org/downloads.html for more

1. Clone the repo using:
   ``` git clone <<curr-repo.git>> ```

3. Build the docker image:
   ```docker compose build```

4. Run ```spark_stream.py``` to set up the spark connection and cassandra cluster

5. Access the aiflow UI:
   ```http://localhost:8080```

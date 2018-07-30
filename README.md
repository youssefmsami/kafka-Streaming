# kafka-Streaming
<br>Live Streaming Solution for Web logs using Kafka, spark-streaming and bokeh</br>
<br>This solution workflow is divided into 3 main tasks:</br>

<br>1- Random webserver log generator using python into a kafka producer </br>
<br>2- A spark-streaming code that consume the produced logs and count the number of ips accessed in a specific time window and produce a new kafka.</br>
<br>3- A python bokeh app that consumes the produced output of the spark streaming and tries to plot it.</br>

<br>Steps:</br>
<br>1- start zookeeper: bin/zookeeper-server-start.sh config/zookeeper.properties</br>
<br>2- start kafka: sudo bin/kafka-server-start.sh config/server.properties</br>
<br>3- Create 2 kafka topics: bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic logs</br>
 <br>                       bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic results</br>
<br>4- Run the logs generator code: python kafka_prod_gen_2.py</br>
<br>5- Run the spark-streaming code: spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 spark-kafka-web-logs-last.py</br>
<br>6- Run the bokeh App:python -m bokeh serve --show  boke_app.py --allow-websocket-origin=[ip]:5006</br>

<br>Attention this part of the bokeh app was displaying blank screen: although if you used the consumer to read the new produced data it displays</br>
<br>the time and the number of users: </br>
<br>bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic results --from-beginning</br>

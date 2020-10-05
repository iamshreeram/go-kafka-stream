# go-kafka-stream
Streaming kafka messages from one broker to another

### Manifesto
Get the messages from source broker, source topic and stream it to destination broker and destination topic without any lag. 

### Usage :

If you want to use as code  
```
go run main.go -sh source-host-broker:9092 -st source-topic -dh dest-host-broker:9092 -dt destn-topic
```

To build the app and run anywhere, 
```
go build -o stream
stream -sh source-host-broker:9092 -st source-topic -dh dest-host-broker:9092 -dt destn-topic
```

#### Sample :

**Source Kafka Broker** : 19.216.80.1:9092  
**Source Kafka Topic** : source_topic_name  
**Destn Kafka Broker** : 201.16.28.13:9092  
**Destn Kafka Topic** : destn_topic_name  

`go run main.go -sh 19.216.80.1:9092 -st source_topic_name -dh 201.16.28.13:9092 -dt destn_topic_name`

# BolzanoParkingStations
This project aims to demonstrate how to extend the capacity of Parking stations in bolzano using nearby e-charging stations smartly through the real time processing of captured measures at the stations.

### Prerequisites to execute the project: 
* Java 8, with Java_Home variable set properly.
* Intellij
* Kafka 2.12-2.5.0 set up properly.
* Elastisearch 7.14, [with trial version started](https://www.elastic.co/guide/en/elasticsearch/reference/current/start-trial.html) with default configuration.
* Kibana (set up and linked with the Elasticserch server) or any other tool (Postman for instance) to send Http requests.
* Node.js 14.17.5

### Steps to run the project.
1. Start Elasticsearch
2. Create the required indices with the following comands (from Kibana dev tools):
PUT /parking_metadata  
PUT /e_charging_metadata  
PUT /misc  
  
PUT /parking_measurements  
{     
  "mappings": {  
    "properties": {  
      "code":    { "type": "text" },    
      "validTime" : {"type": "date", "format": "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"},  
      "value":   { "type": "long" },  
	  "is_read":  { "type": "text" }  	  
    }  
  }  
}    

  
PUT /e_charging_measurements
{  
  "mappings": {  
    "properties": {  
      "code":    { "type": "text" },    
      "validTime" : {"type": "date", "format": "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"},  
      "value":   { "type": "long" },  
	  "is_read":  { "type": "text" }	    
    }  
  }  
}  
3. Head to the Elasticsearch_script directory and copy the commands to create Elasticsearch watchers in Kibana dev tools, run it to create the watchers.  
4. Head to the node_server directory in the terminal and execute the command : node application.js, it will start the server and set it to listen on localhost:3000.  
5. Start zookeeper then Kafka Server  
6. Execute the following commands in termain to create the required topics:  
* kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --create --topic EChargingStations_metadata --partitions 1  
* kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --create --topic ParkingStations_metadata --partitions 1  
* kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --create --topic EChargingStations_available_measurements --partitions 1  
* kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --create --topic ParkingStations_occupation_measurements --partitions 1  
7. Open the project in Intellij, head to APIConnector-KafkaProducer module, then in the RestAPIConnector run the classes : StationProducer (just once or everytime it's needed), ParkingStationMeasurementProducer, EChargingStationMeasurementProducer.  
8. In intellij, head to KafkaConsumer-Elasticsearch module, then in the ElasticSearchConsumers package run the class : StationConsumer  
9. Head to "http://localhost:3000/" to visualise the simple GUI.

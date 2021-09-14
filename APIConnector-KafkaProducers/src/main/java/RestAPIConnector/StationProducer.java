package RestAPIConnector;

import StationSerializers.StationSerializer;
import StationsBusinessLogic.Station;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class StationProducer {
    private final String host ;
    private final String header ;
    private final String value ;

    Logger logger = LoggerFactory.getLogger(StationProducer.class.getName());

    public StationProducer(String host, String header, String value){
        this.host = host;
        this.header = header;
        this.value = value;
    }

    public void run(){

        logger.info("Setup station producer");

        //Create API client
        APIClient client = new APIClient(host,header,value);

        //Create producer
        KafkaProducer<String, Station> producer = createKafkaProducer();

        List<String> stationTypes = Arrays.asList("ParkingStation","EChargingStation");

        for(String stationType : stationTypes){

            //Set the topic
            String topic = stationType+ "s_metadata";

            //Send data to kafka
            try {
                List<Station> stations =
                        client.fetchStations(stationType,"smetadata.capacity");
                for(Station s: stations){
//                    System.out.println(s);
                    producer.send(new ProducerRecord<>(topic, s.getCode(), s), (recordMetadata, e) -> {
                        if (e != null) {
                            logger.error("An error occurred: ", e);
                        }
                        else{
                            // the record was successfully sent
                            logger.info("Received new metadata. \n" +
                                    "Topic:" + recordMetadata.topic() + "\n" +
                                    "Partition: " + recordMetadata.partition() + "\n" +
                                    "Offset: " + recordMetadata.offset());
                        }
                    });
                }
                // Flushing data, to force it to appear
                producer.flush();

            } catch (UnirestException e) {
                logger.error("Could not retrieve "+ stationType + "s: " + e.getMessage());
            }
        }

        // Closing producer
        producer.close();

        logger.info("End of production of stations' metadata");
    }

    public KafkaProducer<String, Station> createKafkaProducer() {

        String bootstrapServer = "127.0.0.1:9092";

        // Create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StationSerializer.class.getName());

        // Setting up the Producer
        return new KafkaProducer<>(properties);
    }

    public static void main(String[] args) {
        String host = "https://mobility.api.opendatahub.bz.it/v2/flat/";

        // Kafka server
        String bootstrapServers = "127.0.0.1:9092";

        // Headers for a request
        String header = "accept";
        String value = "application/json";


        new StationProducer(host, header, value).run();

    }


}

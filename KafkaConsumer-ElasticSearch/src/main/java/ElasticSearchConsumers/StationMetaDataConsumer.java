package ElasticSearchConsumers;

import StationDeserializers.StationDeserializer;
import StationsBusinessLogic.Station;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


public class StationMetaDataConsumer {

    private final Logger logger;
    private final String bootstrapServers;
    private final String group_ID;
    private final String deserializer;
    private final List<String> stationTypes;
    private final KafkaConsumer<String, Station> consumer;
    private RestHighLevelClient elasticSearchClient;

    public StationMetaDataConsumer(String bootstrapServers, String group_ID, List<String> stationTypes,
                                   RestHighLevelClient elasticSearchClient){

        logger = LoggerFactory.getLogger(this.getClass().getName());
        this.bootstrapServers = bootstrapServers;
        this.group_ID = group_ID;
        deserializer = StationDeserializer.class.getName();
        this.stationTypes = stationTypes;
        this.elasticSearchClient = elasticSearchClient;
        consumer = createStationKafkaConsumer();

    }

    public void run(){

        //Creating the ObjectMapper object
        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();

        // We want to read the meta data from the beginning, we use seekToBeginning
        consumer.seekToBeginning(consumer.assignment());

        // Poll for data
        logger.info("Polling stations metadata");
        ConsumerRecords<String, Station> records = consumer.poll(Duration.ofSeconds(5));

        int recordCount = records.count();
        logger.info("Received " + recordCount + " records");

        BulkRequest bulkRequest = new BulkRequest();
        String index;

        for (ConsumerRecord<String, Station> record : records){

            //Do something , for now we print
            logger.info("Topic: " + record.topic() + ", Key: " + record.key());
            logger.info("Value: " + record.value());
            logger.info("Partition: "+ record.partition() + ", Offset: " + record.offset());

            if(record.topic().equals("ParkingStations_metadata")){
                index = "parking_metadata";
            }else{
                if(record.topic().equals("EChargingStations_metadata")) {
                    index = "e_charging_metadata";}
                else{
                    index = "misc";
                }
            }

            try {
                String id = record.value().getCode();
                logger.info("Inserting document : " +index +"/_doc/"+ id);

                //Converting the Station to JSONString
                String jsonStation = ow.writeValueAsString(record.value());

                //Prepare data for bulk insert into ElasticSearch
                bulkRequest.add(new IndexRequest(index).id(id).source(jsonStation, XContentType.JSON));

            } catch (NullPointerException e){
                logger.warn("Skipping bad data: " + record.value());
            }
            catch (JsonProcessingException e) {
                logger.warn("Count not parse this object to JSON: " + e.getMessage());
            }
        }

        // We only commit if we recieved something
        // Bulk Insert of Data
        try{
            if (recordCount > 0) {
                BulkResponse bulkItemResponses = elasticSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                logger.info("Committing offsets...");
                consumer.commitSync();
                logger.info("Offsets have been committed");
                Thread.sleep(500);
            }

        }
        catch(IOException e){
            logger.error("IO Exception while saving data into ElasticSearch " + e.getMessage());
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        consumer.close();

    }

    public KafkaConsumer<String, Station> createStationKafkaConsumer(){

        // Create consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,group_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        // Create consumer
        KafkaConsumer<String, Station> consumer = new KafkaConsumer<>(properties);

        //Set the topics
        List<String> topics = new ArrayList<>();
        for(String stationType: stationTypes){
            topics.add(stationType+ "s_metadata");
        }

        // Subscribe to topics
        consumer.subscribe(topics);

        return consumer;
    }

//    public static void main(String[] args) throws IOException {
//
//        RestHighLevelClient elasticSearchClient = createClient() ;
//
//        String bootstrapServers = "127.0.0.1:9092";
//
//        List<String> stationTypes = Arrays.asList("ParkingStation","EChargingStation");
//
//        String group_ID = "stations_consumers";
//
//        new StationMetaDataConsumer(bootstrapServers, group_ID, stationTypes, elasticSearchClient).run();
//
//        elasticSearchClient.close();
//
//        }


    }


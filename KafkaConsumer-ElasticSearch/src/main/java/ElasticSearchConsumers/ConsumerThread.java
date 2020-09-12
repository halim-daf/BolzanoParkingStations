package ElasticSearchConsumers;

import StationsBusinessLogic.StationMeasurement;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
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
import java.time.ZoneOffset;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerThread implements Runnable{

    private final CountDownLatch latch;
    private final KafkaConsumer<String, StationMeasurement> consumer;
    private final RestHighLevelClient elasticSearchClient;
    private final Logger logger ;

    public ConsumerThread(CountDownLatch latch, String bootstrapServers, String classDeserializer,
                          String group_ID, List<String> topics, RestHighLevelClient elasticSearchClient){

        logger = LoggerFactory.getLogger(ConsumerThread.class.getName());
        this.elasticSearchClient = elasticSearchClient;

        this.latch = latch;

        // Create consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classDeserializer);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,group_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");

        // Create consumer
        consumer = new KafkaConsumer<>(properties);

        // Subscribe to topic
        consumer.subscribe(topics);

    }

    @Override
    public void run() {

        try{
            //Creating the ObjectMapper object
            ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();

            while(true){
                ConsumerRecords<String, StationMeasurement> records = consumer.poll(Duration.ofMillis(100));

                int recordCount = records.count();
                logger.info("Received " + recordCount + " records");

                BulkRequest bulkRequest = new BulkRequest();
                String index;

                for (ConsumerRecord<String, StationMeasurement> record : records){

                    //Do something , for now we print
                    logger.info("Topic: " + record.topic() + ", Key: " + record.key());
                    logger.info("Value: " + record.value());
                    logger.info("Partition: "+ record.partition() + ", Offset: " + record.offset());

                    if(record.topic().equals("ParkingStations_occupation_measurements")){
                        index = "parking_measurements";
                    }else{
                        if(record.topic().equals("EChargingStations_available_measurements")){
                            index = "e_charging_measurements";
                        }
                        else{
                            index = "misc";
                        }
                    }

                    try {
                        String id = record.value().getCode() + "_"
                                + record.value().getValidTime().toEpochSecond(ZoneOffset.UTC);
                        logger.info("Inserting document : " +index +"/_doc/"+ id);

                        //Converting the Station to JSONString
                        String jsonMeasurement = ow.writeValueAsString(record.value());

                        //Prepare data for bulk insert into ElasticSearch
                        bulkRequest.add(new IndexRequest(index).id(id).source(jsonMeasurement, XContentType.JSON));

                    } catch (NullPointerException e){
                        logger.warn("skipping bad data: " + record.value());
                    }


                }
                // We only commit if we recieved something
                // Bulk Insert of Data
                if (recordCount > 0) {
                    BulkResponse bulkItemResponses = elasticSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    logger.info("Committing offsets...");
                    consumer.commitSync();
                    logger.info("Offsets have been committed");
                    try {
                        Thread.sleep(30*1000); // 30 seconds
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

            }
        }
        catch(WakeupException e){
            logger.info("Received shutdown signal !");
        }
        catch(IOException e){
            logger.error("IO Exception while saving data into ElasticSearch " + e.getMessage());
        }
        finally {
            consumer.close();
            // Tell the parent thread that we're done with the consumer
            latch.countDown();
        }
    }

    public void shutdown(){
        // Interrupting the poll of messages
        consumer.wakeup();
    }

}

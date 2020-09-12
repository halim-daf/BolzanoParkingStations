package RestAPIConnector;

import StationsBusinessLogic.StationMeasurement;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ProducerThread implements Runnable{

    private final String topic;
    private final String stationType;
    private final String dataType;
    private final APIClient client;
    private final int sleepDuration;
    private final CountDownLatch latch;
    private boolean stopFlag;
    private final KafkaProducer<String, StationMeasurement> producer;
    private final Logger logger = LoggerFactory.getLogger(ProducerThread.class.getName());

    public ProducerThread(String bootstrapServers, String classSerializer, APIClient client,
                          String topic,String stationType, String dataType, CountDownLatch latch , int sleepDuration) {

        this.client = client;
        this.stationType = stationType;
        this.dataType = dataType;
        this.sleepDuration = sleepDuration; // In seconds
        this.latch = latch;
        this.topic = topic;
        stopFlag = false;

        // Create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classSerializer);

        // Setting up the Producer
        producer = new KafkaProducer<>(properties);
    }


    @Override
    public void run() {

        logger.info("Running thread for \"" + dataType + "\" measurements");

        try{
            while(!stopFlag) {
                try {
                    List<StationMeasurement> measurements = client.fetchAllStationsMeasurement(stationType, dataType);
                    for(StationMeasurement measurement : measurements ){
                        System.out.println(measurement);
                        producer.send(new ProducerRecord<>(topic, measurement.getCode(), measurement),
                                (recordMetadata, e) -> {
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
                    System.out.println("Sent "+ measurements.size() +" measurements");

                    // Flushing data, to force it to appear
                    producer.flush();

                    // Measurements are produced every 'N' minutes, so we also execute this loop every 'N' - 2  minutes
                    // Thread.sleep takes a duration in ms as an input, hence the conversion.
                    Thread.sleep(sleepDuration * 1000);

            }
                catch(UnirestException e){
                    logger.error("Could not retrieve latest measurement : " + e.getMessage());
                }
                catch (InterruptedException e){
                    logger.error("Producer thread interrupted : "+ e.getMessage());
                }
            }
        }
        catch(WakeupException e){
            logger.info("Received shutdown signal !");
        }
        finally {
            // Stop producing
            producer.close();
            // Tell the parent thread that we're done with the producer
            latch.countDown();
        }
    }

    public void shutdown(){
        // Interrupting the poll of messages
        stopFlag = true;
    }
}

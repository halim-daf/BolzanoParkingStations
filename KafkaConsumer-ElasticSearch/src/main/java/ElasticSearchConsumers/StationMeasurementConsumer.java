package ElasticSearchConsumers;

import StationDeserializers.StationMeasurementDeserializer;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;


public class StationMeasurementConsumer {

    private final String bootstrapServers;
    private final String classDeserializer;
    private final Logger logger;
    private final RestHighLevelClient elasticSearchClient;

    public StationMeasurementConsumer(String bootstrapServers,RestHighLevelClient elasticSearchClient){

        this.bootstrapServers = bootstrapServers;
        classDeserializer = StationMeasurementDeserializer.class.getName();
        logger = LoggerFactory.getLogger(this.getClass().getName());
        this.elasticSearchClient = elasticSearchClient;
    }

    public void produceMeasurementsForStations(String group_ID, List<String> topics){

        // Latch to deal with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        // Create the consumer runnable
        logger.info("Creating the consumer thread");
        Runnable stationMeasurementConsumerRunnable = new ConsumerThread(latch, bootstrapServers,
                classDeserializer, group_ID, topics, elasticSearchClient);

        // Start the consumer thread
        Thread stationMeasurementThread = new Thread(stationMeasurementConsumerRunnable);
        stationMeasurementThread.start();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught shutdown hook");
            ((ConsumerThread) stationMeasurementConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Consumer got interrupted", e);
        } finally {
            logger.info("Consumer is closing");
        }
                }

//    public static void main(String[] args) {
//
//        String bootstrapServers = "127.0.0.1:9092";
//        RestHighLevelClient elasticSearchClient = createClient() ;
//        String group_ID ="measurement_consumers";
//        List<String> topics = Arrays.asList("ParkingStations_occupation_measurements",
//                "EChargingStations_available_measurements");
//        new StationMeasurementConsumer(bootstrapServers,elasticSearchClient)
//                .produceMeasurementsForStations(group_ID, topics);
//    }




    }








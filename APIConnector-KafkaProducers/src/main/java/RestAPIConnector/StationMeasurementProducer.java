package RestAPIConnector;

import StationSerializers.StationMeasurementSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class StationMeasurementProducer {

    private String host ;
    private String header ;
    private String value ;
    private String bootstrapServers;
    private String classSerializer;
    private Logger logger;


    public StationMeasurementProducer(String host, String header, String value, String bootstrapServers){
        this.host = host;
        this.header = header;
        this.value = value;
        this.bootstrapServers = bootstrapServers;
        this.classSerializer = StationMeasurementSerializer.class.getName();
        logger = LoggerFactory.getLogger(this.getClass().getName());
    }

    public void produceMeasurementsForStations(String stationType, String dataType, String topic, int sleepDuration){

//        Create API client
        APIClient client = new APIClient(host,header,value);

        // Latch to deal with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        // Create the producer runnable
        logger.info("Creating the producer thread for data type: " + dataType);
        ProducerThread stationMeasurementRunnable = new ProducerThread(bootstrapServers, classSerializer, client,
                topic, stationType, dataType, latch, sleepDuration);

        // Start the producer thread
        Thread stationMeasurementThread = new Thread(stationMeasurementRunnable);
        stationMeasurementThread.start();

        // Add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught shutdown hook for thread of  station type: " + stationType + " data type: " + dataType);
            stationMeasurementRunnable.shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Producer for station type " + stationType + " data type: " + dataType + " has exited");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Producer for station type: " + stationType + " data type: " + dataType +" got interrupted.", e);
        } finally {
            logger.info("Producer for station type: " + stationType + " data type: " + dataType +" is closing.");
        }
    }


//    public static void main(String[] args) {
//        String host = "https://mobility.api.opendatahub.bz.it/v2/flat/";
//
//        // Headers for a request
//        String header = "accept";
//        String value = "application/json";
//
//        // Kafka server
//        String bootstrapServers = "127.0.0.1:9092";
//
//        // Frequency of polls in seconds , N=5 => N-2 = 3,  we want to poll each 3 minutes
//        int sleepDuration = 3*60;
//
//        //Data producing details
//        String dataType = "occupied";
//        String stationType = "Station";
//
//        String topic = "ParkingStations_occupation_measurements";
//
//
//        StationMeasurementProducer producer =new StationMeasurementProducer(host, header, value, bootstrapServers);
//        producer.produceMeasurementsForStations(stationType, dataType, topic, sleepDuration);
//
//    }
}

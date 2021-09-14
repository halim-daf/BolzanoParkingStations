package RestAPIConnector;

public class ParkingStationMeasurementProducer {

    public static void main(String[] args) {
        String host = "https://mobility.api.opendatahub.bz.it/v2/flat/";

        // Headers for a request
        String header = "accept";
        String value = "application/json";

        // Kafka server
        String bootstrapServers = "127.0.0.1:9092";

        // Frequency of polls in seconds
        int sleepDuration = 2*60;


        //Data producing details
        String dataType = "occupied";
        String stationType = "ParkingStation";

        String topic = "ParkingStations_occupation_measurements";


        StationMeasurementProducer producer =new StationMeasurementProducer(host, header, value, bootstrapServers);
        producer.produceMeasurementsForStations(stationType, dataType, topic, sleepDuration);
    }
}

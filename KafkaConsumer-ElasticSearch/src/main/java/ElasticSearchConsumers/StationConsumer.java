package ElasticSearchConsumers;

import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static ElasticSearchConsumers.ElasticSearchClient.createClient;

public class StationConsumer {
    public static void main(String[] args) {

        RestHighLevelClient elasticSearchClient = createClient() ;

        String bootstrapServers = "127.0.0.1:9092";

        String stations_group_ID = "stations_consumers";
        String measurements_group_ID ="measurement_consumers";

        List<String> stationTypes = Arrays.asList("ParkingStation","EChargingStation");
        List<String> measurement_topics = Arrays.asList("ParkingStations_occupation_measurements",
                "EChargingStations_available_measurements");

        new StationMetaDataConsumer(bootstrapServers, stations_group_ID, stationTypes, elasticSearchClient).run();

        new StationMeasurementConsumer(bootstrapServers,elasticSearchClient)
                .produceMeasurementsForStations(measurements_group_ID, measurement_topics);

        try {
            elasticSearchClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}

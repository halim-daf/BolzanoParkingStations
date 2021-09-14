package RestAPIConnector;

import StationsBusinessLogic.Station;
import StationsBusinessLogic.StationMeasurement;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.json.JSONArray;
import org.json.JSONObject;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class APIClient {

    private String host;
    private String header;
    private String header_value;

    public APIClient(String host, String header, String value) {
        this.host = host;
        this.header = header;
        this.header_value = value;
    }

    private JSONArray getStations (String stationType, String extra_metadata) throws UnirestException {
        JSONArray stationsArray = null;

        HttpResponse<JsonNode> jsonResponse = null;
        String station_url = host + stationType;
        String parameters = "scode,sname,scoordinate,"+extra_metadata;
        String whereCondition = "or(smetadata.municipality.eq.Bolzano - Bozen" +
                ",smetadata.city.eq.Bozen" +
                ",smetadata.city.eq.Bolzano - Bozen" +
                ",smetadata.city.eq.BOLZANO - BOZEN)";
        jsonResponse = Unirest.get(station_url)
                .header(header, header_value)
                .queryString("select",parameters)
                .queryString("where",whereCondition)
//                .queryString("where","smetadata.municipality.eq.Bolzano - Bozen")
                .queryString("limit", -1)
                .queryString("distinct", "true")
                .asJson();

        if (jsonResponse != null && jsonResponse.getStatus() == 200) {
            stationsArray = jsonResponse.getBody().getObject().getJSONArray("data");
        }else{
            System.out.println("Could not retrieve stations");
        }
        return stationsArray;
    }

    private JSONArray getStationLatestMeasurement (String stationCode,String dataType) throws UnirestException {
        JSONArray measurementArray = null;

        HttpResponse<JsonNode> jsonResponse = null;
        String station_url = host + "*/" + dataType + "/latest";
        String parameters = "mvalue,scode,mvalidtime";
        String whereCondition = "scode.eq.\""+stationCode +"\"";
        jsonResponse = Unirest.get(station_url)
                .header(header, header_value)
                .queryString("select",parameters)
                .queryString("where",whereCondition)
                .asJson();

        if (jsonResponse != null && jsonResponse.getStatus() == 200) {
            measurementArray = jsonResponse.getBody().getObject().getJSONArray("data");
        }else{
            System.out.println("Could not retrieve latest measurement !");
        }
        return measurementArray;
    }

    private JSONArray getAllStationsLatestMeasurements (String stationType, String dataType) throws UnirestException {
        JSONArray measurementArray = null;

        HttpResponse<JsonNode> jsonResponse = null;
        String station_url = host + stationType + "/" + dataType + "/latest";
        String parameters = "mvalue,scode,mvalidtime";
        String whereCondition = "or(smetadata.municipality.eq.Bolzano - Bozen" +
                ",smetadata.city.eq.Bozen" +
                ",smetadata.city.eq.Bolzano - Bozen" +
                ",smetadata.city.eq.BOLZANO - BOZEN)";
        jsonResponse = Unirest.get(station_url)
                .header(header, header_value)
                .queryString("select",parameters)
                .queryString("where",whereCondition)
                .asJson();

        if (jsonResponse != null && jsonResponse.getStatus() == 200) {
            measurementArray = jsonResponse.getBody().getObject().getJSONArray("data");
        }else{
            System.out.println("Could not retrieve latest measurement !");
        }
        return measurementArray;
    }

    public List<Station> fetchStations(String stationType, String extra_metadata) throws UnirestException {

        List<Station> stations = new ArrayList<Station>();
        JSONArray stationsArray = this.getStations(stationType, extra_metadata);

        if(stationsArray.length()!=0) {
            for (Object station : stationsArray) {
                JSONObject json_station = (JSONObject) station;
                JSONObject json_coordinates = (JSONObject) json_station.get("scoordinate");
                try{
                    stations.add(new Station(
                            json_station.getString("scode"),
                            json_station.getString("sname"),
                            json_station.getInt("smetadata.capacity"),
                            json_coordinates.getDouble("x"),
                            json_coordinates.getDouble("y"),
                            json_coordinates.getInt("srid")));
                }
                catch(Exception e){
                    //If the station is misssing metadatan, we are not interested in it;
                    continue;
                }

            }
        }
        return stations;
    }

    public List<StationMeasurement> fetchAllStationsMeasurement(String stationType, String dataType) throws UnirestException {

        List<StationMeasurement> measurements = new ArrayList<>();
        JSONArray measurementArray = this.getAllStationsLatestMeasurements(stationType, dataType);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSZ");

        if(measurementArray!= null && measurementArray.length()!=0) {
            for (Object m : measurementArray) {
                JSONObject json_measurement = (JSONObject) m;
                measurements.add(new StationMeasurement(
                        json_measurement.getString("scode"),
                        json_measurement.getInt("mvalue"),
                        LocalDateTime.parse(json_measurement.getString("mvalidtime"),formatter)));
            }
        }
        else{
            System.out.println("No data retrieved from mobility API !");
        }
        return measurements;
    }

    public StationMeasurement fetchStationMeasurement(String stationCode, String dataType) throws UnirestException {

        StationMeasurement measurement = null;
        JSONArray measurementArray = this.getStationLatestMeasurement(stationCode,dataType);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSZ");

        if(measurementArray.length()!=0) {
            JSONObject json_measurement = measurementArray.getJSONObject(0);
            measurement = new StationMeasurement(
                    json_measurement.getString("scode"),
                    json_measurement.getInt("mvalue"),
                    LocalDateTime.parse(json_measurement.getString("mvalidtime"),formatter));
        }
        else{
            System.out.println("No data retrieved from mobility API !");
        }
        return measurement;
    }


//    public static void main(String[] args) throws UnirestException{
//        // Host url
//        String host = "https://mobility.api.opendatahub.bz.it/v2/flat/";
//
//        // Headers for a request
//        String header = "accept";
//        String value = "application/json";
//
//        APIClient client =  new APIClient(host,header,value);
//        List<Station> parkingStations = client.fetchStations("ParkingStation","smetadata.capacity");
//        List<Station> eChargingStations = client.fetchStations("EChargingStation","smetadata.capacity");
//
//        System.out.println(parkingStations.size());
//        System.out.println(eChargingStations.size());
//        System.out.println(client.fetchStationMeasurement("103", "occupied"));
//        System.out.println(client.fetchAllStationsMeasurement("ParkingStation","occupied"));

//    }
}



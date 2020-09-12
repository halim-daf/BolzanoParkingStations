package StationDeserializers;

import StationsBusinessLogic.Station;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class StationDeserializer implements Deserializer<Station> {

    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    public Station deserialize(String s, byte[] data) {
        String encoding = "UTF8";
        try {
            if (data == null) {
                System.out.println("Null received at deserialize");
                return null;
            }

            ByteBuffer buf = ByteBuffer.wrap(data);

            int sizeOfCode = buf.getInt();
            byte[] codeBytes = new byte[sizeOfCode];
            buf.get(codeBytes);
            String deserializedCode = new String(codeBytes, encoding);

            int sizeOfName = buf.getInt();
            byte[] nameBytes = new byte[sizeOfName];
            buf.get(nameBytes);
            String deserializedName = new String(nameBytes, encoding);

            int capacity = buf.getInt();
            Double x = buf.getDouble();
            Double y = buf.getDouble();
            int rid = buf.getInt();

            return new Station(deserializedCode,deserializedName,capacity,x,y,rid);

        } catch (Exception e) {
            throw new SerializationException("Error when deserializing byte[] to Station");
        }
    }

    public void close() {

    }
}

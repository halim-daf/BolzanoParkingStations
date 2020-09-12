package StationDeserializers;

import StationsBusinessLogic.StationMeasurement;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;

public class StationMeasurementDeserializer implements Deserializer<StationMeasurement> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public StationMeasurement deserialize(String s, byte[] data) {
        String encoding = "UTF8";
        try{
            if (data == null) {
                System.out.println("Null received at deserialize");
                return null;
            }

            ByteBuffer buf = ByteBuffer.wrap(data);

            int sizeOfCode = buf.getInt();
            byte[] codeBytes = new byte[sizeOfCode];
            buf.get(codeBytes);
            String deserializedCode = new String(codeBytes, encoding);

            int value = buf.getInt();
            LocalDateTime vTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(buf.getLong()), ZoneOffset.UTC);
            return new StationMeasurement(deserializedCode, value, vTime);


        }catch(Exception e){
            throw new SerializationException("Error when deserializing byte[] to StationMeasurement");
        }
    }

    @Override
    public void close() {

    }
}

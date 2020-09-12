package StationSerializers;

import StationsBusinessLogic.StationMeasurement;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.time.ZoneOffset;
import java.util.Map;

public class StationMeasurementSerializer implements Serializer<StationMeasurement> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String s, StationMeasurement stationMeasurement) {
        String encoding = "UTF8";

        int sizeOfCode;
        int sizeOfVDate;
        byte[] serializedCode;

        try {
            if (stationMeasurement == null)
                return null;

            serializedCode = stationMeasurement.getCode().getBytes(encoding);
            sizeOfCode = serializedCode.length;


            ByteBuffer buf = ByteBuffer.allocate(4 + sizeOfCode + 4 + 8 );
            buf.putInt(sizeOfCode);
            buf.put(serializedCode);
            buf.putInt(stationMeasurement.getValue());
            buf.putLong(stationMeasurement.getValidTime().toInstant(ZoneOffset.UTC).getEpochSecond());


            return buf.array();

        }catch(Exception e){
            throw new SerializationException("Error when serializing Supplier to byte[]");
        }
    }

    @Override
    public void close() {

    }
}

package StationSerializers;

import StationsBusinessLogic.Station;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class StationSerializer implements Serializer<Station> {

    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    public byte[] serialize(String s, Station station) {
        String encoding = "UTF8";

        int sizeOfName;
        int sizeOfCode;
        byte[] serializedName;
        byte[] serializedCode;

        try {
            if (station == null)
                return null;

            serializedName = station.getName().getBytes(encoding);
            sizeOfName = serializedName.length;
            serializedCode = station.getCode().getBytes(encoding);
            sizeOfCode = serializedCode.length;

            ByteBuffer buf = ByteBuffer.allocate(4 + sizeOfCode + 4 + sizeOfName + 4 + 8 + 8 + 4);
            buf.putInt(sizeOfCode);
            buf.put(serializedCode);
            buf.putInt(sizeOfName);
            buf.put(serializedName);
            buf.putInt(station.getCapacity());
            buf.putDouble(station.getX());
            buf.putDouble(station.getY());
            buf.putInt(station.getRid());

            return buf.array();

        } catch (Exception e) {
            throw new SerializationException("Error when serializing Station to byte[]");
        }
    }

    public void close() {

    }

}

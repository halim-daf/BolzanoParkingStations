package StationsBusinessLogic;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

public class Station {

    private String code ;
    private String name;
    private int capacity;
    private Double x;
    private Double y;
    private int rid;

    public Station(String code, String name, int capacity, Double x, Double y, int rid) {
        this.code = code;
        this.name = name;
        this.x = x;
        this.y = y;
        this.rid = rid;
        this.capacity = capacity;
    }

    public int getCapacity() {

        return capacity;
    }

    public String getCode(){
        return code;
    }

    public String getName(){
        return name;
    }

    public Double getX() {
        return x;
    }

    public void setX(Double x) {
        this.x = x;
    }

    public Double getY() {
        return y;
    }

    public void setY(Double y) {
        this.y = y;
    }

    public int getRid() {
        return rid;
    }

    public void setRid(int rid) {
        this.rid = rid;
    }

    @Override
    public String toString() {
        return "Station{" +
                "code='" + code + '\'' +
                ", s_name='" + name + '\'' +
                ", s_capacity=" + capacity +
                ", Coordinates{" +
                "'" + x + "'," +
                "'" + y + "'," +
                "'" + rid + "'" +
                '}';
    }


}

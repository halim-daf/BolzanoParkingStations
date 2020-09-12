package StationsBusinessLogic;

import java.time.LocalDateTime;

public class StationMeasurement {
    private String code;
    private int value;
    private LocalDateTime validTime;

    public StationMeasurement(String code, int value, LocalDateTime validTime){
        this.code = code;
        this.value = value;
        this.validTime = validTime;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public LocalDateTime getValidTime() {
        return validTime;
    }

    public void setValidTime(LocalDateTime validTime) {
        this.validTime = validTime;
    }

    @Override
    public String toString() {
        return "StationMeasurement{" +
                "code='" + code + '\'' +
                ", value=" + value +
                ", validTime=" + validTime +
                '}';
    }
}

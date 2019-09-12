package webmedia.cep2019.simplesample.event;

public class SensorUpdate {
    double temperature;
    double humidity;
    int roomId;
    long timestamp;

    public SensorUpdate() {
    }

    public SensorUpdate(double temperature, double humidity, int roomId, long timestamp) {
        this.temperature = temperature;
        this.humidity = humidity;
        this.roomId = roomId;
        this.timestamp = timestamp;
    }

    public double getTemperature() {
        return temperature;
    }

    public void setTemperature(double temperature) {
        this.temperature = temperature;
    }

    public double getHumidity() {
        return humidity;
    }

    public void setHumidity(double humidity) {
        this.humidity = humidity;
    }

    public int getRoomId() {
        return roomId;
    }

    public void setRoomId(int roomId) {
        this.roomId = roomId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}

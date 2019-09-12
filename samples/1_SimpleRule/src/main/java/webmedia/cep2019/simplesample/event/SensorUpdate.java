package webmedia.cep2019.simplesample.event;

public class SensorUpdate {
    double temperature;
    double humidity;
    int roomId;

    public SensorUpdate() {
    }

    public SensorUpdate(double temperature, double humidity, int roomId) {
        this.temperature = temperature;
        this.humidity = humidity;
        this.roomId = roomId;
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
}

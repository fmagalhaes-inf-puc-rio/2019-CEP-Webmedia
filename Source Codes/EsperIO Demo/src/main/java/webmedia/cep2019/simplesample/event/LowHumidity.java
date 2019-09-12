package webmedia.cep2019.simplesample.event;

public class LowHumidity extends SensorUpdate {
    public LowHumidity() {
        super();
    }

    public LowHumidity(double temperature, double humidity, int roomId, long timestamp) {
        super(temperature, humidity, roomId, timestamp);
    }

    public double getHumidityDeviation(){
        return 0.35-this.humidity;
    }
}

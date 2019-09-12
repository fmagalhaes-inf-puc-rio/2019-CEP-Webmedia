package webmedia.cep2019.simplesample.event;

public class LowHumidity extends SensorUpdate {
    public LowHumidity() {
        super();
    }

    public LowHumidity(double temperature, double humidity, int roomId) {
        super(temperature, humidity, roomId);
    }

    public double getHumidityDeviation(){
        return 0.35-this.humidity;
    }
}

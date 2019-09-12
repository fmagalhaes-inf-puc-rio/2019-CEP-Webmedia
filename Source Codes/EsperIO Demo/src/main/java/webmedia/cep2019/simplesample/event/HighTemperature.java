package webmedia.cep2019.simplesample.event;

public class HighTemperature extends SensorUpdate {
    public HighTemperature() {
        super();
    }

    public HighTemperature(double temperature, double humidity, int roomId) {
        super(temperature, humidity, roomId);
    }

    public double getTemperatureDeviation(){
        return this.temperature-35;
    }
}

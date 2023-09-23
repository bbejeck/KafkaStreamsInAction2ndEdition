package bbejeck.chapter_9;

/**
 * User: Bill Bejeck
 * Date: 9/17/23
 * Time: 12:41 PM
 */
public class IotSensorAggregation  {

    private double highestSeen;
    private double temperatureSum;
    private int numberReadings;
    private int tempThresholdExceededCount;
    private double readingThreshold;
    double averageReading;

    public IotSensorAggregation(final double readingThreshold) {
        this.readingThreshold = readingThreshold;
    }

    public IotSensorAggregation() {}

    public double highestSeen() {
        return highestSeen;
    }

    public double temperatureSum() {
        return temperatureSum;
    }

    public void setTemperatureSum(double temperatureSum) {
        this.temperatureSum = temperatureSum;
    }

    public int numberReadings() {
        return numberReadings;
    }

    public void setNumberReadings(int numberReadings) {
        this.numberReadings = numberReadings;
    }

    public int tempThresholdExceededCount() {
        return tempThresholdExceededCount;
    }

    public void setTempThresholdExceededCount(int tempThresholdExceededCount) {
        this.tempThresholdExceededCount = tempThresholdExceededCount;
    }

    public double readingThreshold() {
        return readingThreshold;
    }

    public void setReadingThreshold(double readingThreshold) {
        this.readingThreshold = readingThreshold;
    }

    public double averageReading() {
        return averageReading;
    }

    public void setAverageReading(double averageReading) {
        this.averageReading = averageReading;
    }

    public void setHighestSeen(double highestSeen) {
        this.highestSeen = highestSeen;
    }
}

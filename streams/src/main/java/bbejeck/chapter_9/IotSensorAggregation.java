package bbejeck.chapter_9;

/**
 * User: Bill Bejeck
 * Date: 9/17/23
 * Time: 12:41 PM
 */
public class IotSensorAggregation  {

    double highestSeen;
    double temperatureSum;
    int numberReadings;
    int tempThresholdExceededCount;
    double readingThreshold;
    double averageReading;

    public IotSensorAggregation(final double readingThreshold) {
        this.readingThreshold = readingThreshold;
    }

    public IotSensorAggregation() {}

    public double getHighestSeen() {
        return highestSeen;
    }

    public void setHighestSeen(double highestSeen) {
        this.highestSeen = highestSeen;
    }

    public double getTemperatureSum() {
        return temperatureSum;
    }

    public void setTemperatureSum(double temperatureSum) {
        this.temperatureSum = temperatureSum;
    }

    public int getNumberReadings() {
        return numberReadings;
    }

    public void setNumberReadings(int numberReadings) {
        this.numberReadings = numberReadings;
    }

    public int getTempThresholdExceededCount() {
        return tempThresholdExceededCount;
    }

    public void setTempThresholdExceededCount(int tempThresholdExceededCount) {
        this.tempThresholdExceededCount = tempThresholdExceededCount;
    }

    public double getReadingThreshold() {
        return readingThreshold;
    }

    public void setReadingThreshold(double readingThreshold) {
        this.readingThreshold = readingThreshold;
    }

    public double getAverageReading() {
        return averageReading;
    }

    public void setAverageReading(double averageReading) {
        this.averageReading = averageReading;
    }
}

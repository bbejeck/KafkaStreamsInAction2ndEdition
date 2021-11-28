package bbejeck.data;

import bbejeck.chapter_4.avro.ProductTransaction;
import bbejeck.chapter_4.avro.StockTransaction;
import bbejeck.chapter_6.proto.PurchasedItemProto;
import bbejeck.chapter_6.proto.RetailPurchaseProto;
import bbejeck.chapter_6.proto.SensorProto;
import bbejeck.chapter_8.proto.StockAlertProto;
import com.github.javafaker.Address;
import com.github.javafaker.Business;
import com.github.javafaker.ChuckNorris;
import com.github.javafaker.Commerce;
import com.github.javafaker.Company;
import com.github.javafaker.Faker;
import com.github.javafaker.HarryPotter;
import com.github.javafaker.IdNumber;
import com.github.javafaker.LordOfTheRings;
import com.github.javafaker.Number;
import com.github.javafaker.Shakespeare;
import com.github.javafaker.Stock;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Utility class that provides various methods for generating data
 * used in examples throughout the book
 */
public class DataGenerator {

    private static final Random random = new Random();
    private static List<String> textileTickers;
    private static List<String> techTickers;
    
    static {
        Faker faker = new Faker();
        Stock stock = faker.stock();
        textileTickers = List.of(stock.nyseSymbol(), stock.nyseSymbol(), stock.nyseSymbol());
        techTickers = List.of(stock.nsdqSymbol(), stock.nsdqSymbol(), stock.nsdqSymbol());
        System.out.println(techTickers);
        System.out.println(textileTickers);
    }

    private DataGenerator() {}

    public static Collection<String> generateRandomText() {
        Faker faker = new Faker();
        Shakespeare shakespeare = faker.shakespeare();
        Company company = faker.company();
        Number number = faker.number();
        ChuckNorris chuckNorris = faker.chuckNorris();
        Collection<String> text = new ArrayList<>();
        int randomTextCount = 10;
        for (int i = 0; i < randomTextCount; i++) {
            int whichFaker = number.numberBetween(1, 6);
            switch (whichFaker) {
                case 1 -> text.add(shakespeare.hamletQuote());
                case 2 -> text.add(shakespeare.asYouLikeItQuote());
                case 3 -> text.add(company.bs());
                case 4 -> text.add(chuckNorris.fact());
            }
        }
        return text;
    }

    public static Collection<StockAlertProto.StockAlert> generateStockAlerts(int numberRecords) {
        Faker faker = new Faker();
        int counter = 0;
        final List<StockAlertProto.StockAlert> stockAlerts = new ArrayList<>();
        Number number = faker.number();
        String textiles = "textiles";
        String technology = "technology";
        StockAlertProto.StockAlert.Builder stockAlertBuilder = StockAlertProto.StockAlert.newBuilder();
        while (counter++ < numberRecords) {
            int tickerIndex = random.nextInt(2);
            //Add textile stock alert
            stockAlertBuilder.setSharePrice(number.randomDouble(2, 1, 5))
                    .setShareVolume(number.numberBetween(1, 100))
                    .setMarketSegment(textiles)
                    .setSymbol(textileTickers.get(tickerIndex));
            stockAlerts.add(stockAlertBuilder.build());
            // Add tech sector alert
            stockAlertBuilder.setMarketSegment(technology);
            stockAlertBuilder.setSymbol(techTickers.get(tickerIndex));
            stockAlerts.add(stockAlertBuilder.build());
        }
        return stockAlerts;
    }


    public static Collection<ProductTransaction> generateProductTransactions(int numberRecords) {
        Faker faker = new Faker();
        int counter = 0;
        final List<ProductTransaction> transactions = new ArrayList<>();
        Commerce commerce = faker.commerce();
        Company company = faker.company();
        Number number = faker.number();
        ProductTransaction.Builder builder = ProductTransaction.newBuilder();
        Instant instant = Instant.now();

        while (counter++ < numberRecords) {
            builder.setCustomerName(company.name())
                    .setPrice(Double.parseDouble(commerce.price()))
                    .setProductName(commerce.productName())
                    .setTimestamp(instant.plusSeconds(5L * counter).truncatedTo(ChronoUnit.SECONDS).toEpochMilli())
                    .setQuantity(number.numberBetween(1, 1000));
            transactions.add(builder.build());
        }
        return transactions;
    }

    public static Map<String, List<SensorProto.Sensor>> generateSensorReadings(int numberRecords) {
        Faker faker = new Faker();
        final List<String> idNumbers = new ArrayList<>();
        IdNumber idNumber = faker.idNumber();
        for (int i = 0; i < 20; i++) {
            idNumbers.add(idNumber.invalid());
        }
        int numberPerTopic = numberRecords / 3;

        Number number = faker.number();
        SensorProto.Sensor.Builder sensorBuilder = SensorProto.Sensor.newBuilder();
        List<SensorProto.Sensor.Type> types = List.of(SensorProto.Sensor.Type.TEMPERATURE, SensorProto.Sensor.Type.PROXIMITY);
        Map<String, List<SensorProto.Sensor>> recordsMap = new HashMap<>();
        List<String> sensorTopics = List.of("combined-sensors", "temperature-sensors", "proximity-sensors");
        sensorTopics.forEach(sensorTopic -> recordsMap.put(sensorTopic, new ArrayList<>()));
        sensorTopics.forEach(sensorTopic -> {
            List<SensorProto.Sensor> sensorList = recordsMap.get(sensorTopic);
            for (int i = 0; i < numberPerTopic; i++) {
                sensorBuilder.setReading(number.randomDouble(2, 1, 1000));
                sensorBuilder.setId(idNumbers.get(random.nextInt(20)));
                SensorProto.Sensor.Type type = switch (sensorTopic) {
                    case "combined-sensors" -> types.get(number.numberBetween(0, 2));
                    case "temperature-sensors" -> SensorProto.Sensor.Type.TEMPERATURE;
                    case "proximity-sensors" -> SensorProto.Sensor.Type.PROXIMITY;
                    default -> types.get(number.numberBetween(0, 2));
                };
                sensorBuilder.setSensorType(type);
                sensorList.add(sensorBuilder.build());
                sensorBuilder.clear();
            }
        });

        return recordsMap;
    }

    public static Collection<RetailPurchaseProto.RetailPurchase> generatePurchasedItems(final int numberRecords) {
        int counter = 0;
        final List<RetailPurchaseProto.RetailPurchase> retailPurchases = new ArrayList<>();
        Faker faker = new Faker();
        Commerce commerce = faker.commerce();
        Number number = faker.number();
        Business business = faker.business();
        IdNumber idNumber = faker.idNumber();
        Address address = faker.address();
        Instant instant = Instant.now();

        PurchasedItemProto.PurchasedItem.Builder purchaseItemBuilder = PurchasedItemProto.PurchasedItem.newBuilder();
        RetailPurchaseProto.RetailPurchase.Builder retailBuilder = RetailPurchaseProto.RetailPurchase.newBuilder();
        List<String> departments = new ArrayList<>(List.of("coffee", "electronics"));
        while (counter++ < numberRecords) {
            int numberOfPurchasedItems = number.numberBetween(1, 6);
            long purchaseDate = instant.plusSeconds(number.randomNumber()).truncatedTo(ChronoUnit.SECONDS).toEpochMilli();
            String customerId = idNumber.invalid();
            for (int i = 0; i < numberOfPurchasedItems; i++) {
                purchaseItemBuilder.setPrice(Double.parseDouble(commerce.price()));
                purchaseItemBuilder.setItem(commerce.productName());
                purchaseItemBuilder.setCustomerId(customerId);
                purchaseItemBuilder.setQuantity(number.numberBetween(1, 100));
                purchaseItemBuilder.setPurchaseDate(purchaseDate);
                retailBuilder.addPurchasedItems(purchaseItemBuilder.build());
            }
            if (departments.size() == 2) {
                departments.add(2, commerce.department());
            } else {
                departments.set(2, commerce.department());
            }
            retailBuilder.setDepartment(departments.get(random.nextInt(3)));
            retailBuilder.setStoreId(commerce.department());
            retailBuilder.setEmployeeId(idNumber.invalid());
            retailBuilder.setCustomerId(customerId);
            retailBuilder.setCreditCardNumber(business.creditCardNumber());
            retailBuilder.setZipCode(address.zipCode());
            retailBuilder.setPurchaseDate(purchaseDate);
            retailPurchases.add(retailBuilder.build());
            retailBuilder.clearPurchasedItems();
        }
        return retailPurchases;
    }


    public static Collection<StockTransaction> generateStockTransaction(int numberRecords) {
        int counter = 0;
        final List<StockTransaction> transactions = new ArrayList<>();
        List<String> symbols = getSymbols(10);
        List<String> brokers = getLordOfTheRingsCharacters(5);
        List<String> customers = getHarryPotterCharacters(10);
        StockTransaction.Builder builder = StockTransaction.newBuilder();
        Instant instant = Instant.now();
        while (counter++ < numberRecords) {
            builder.setCustomerName(customers.get(random.nextInt(customers.size())))
                    .setBroker(brokers.get(random.nextInt(brokers.size())))
                    .setSymbol(symbols.get(random.nextInt(symbols.size())))
                    .setTimestamp(instant.plusSeconds(5L * counter).truncatedTo(ChronoUnit.SECONDS).toEpochMilli())
                    .setShares(random.nextInt(25_000))
                    .setSharePrice(random.nextDouble() * 100);

            transactions.add(builder.build());
        }

        return transactions;
    }


    private static List<String> getLordOfTheRingsCharacters(int number) {
        LordOfTheRings lordOfTheRings = new Faker().lordOfTheRings();
        List<String> characters = new ArrayList<>();
        for (int i = 0; i < number; i++) {
            characters.add(lordOfTheRings.character());
        }
        return characters;
    }

    private static List<String> getHarryPotterCharacters(int number) {
        HarryPotter harryPotter = new Faker().harryPotter();
        List<String> characters = new ArrayList<>();
        for (int i = 0; i < number; i++) {
            characters.add(harryPotter.character());
        }
        return characters;
    }

    private static List<String> getSymbols(int number) {
        Stock stock = new Faker().stock();
        List<String> symbols = new ArrayList<>();
        for (int i = 0; i < number; i++) {
            symbols.add(stock.nsdqSymbol());
        }
        return symbols;
    }

    public static void main(String[] args) {
        DataGenerator.generateProductTransactions(10).forEach(System.out::println);
        DataGenerator.generateStockTransaction(10).forEach(System.out::println);
        DataGenerator.generatePurchasedItems(10).forEach(System.out::println);
        DataGenerator.generateRandomText().forEach(System.out::println);
        DataGenerator.generateSensorReadings(10).entrySet().forEach(System.out::println);
        DataGenerator.generateStockAlerts(4).forEach(System.out::println);
    }
}

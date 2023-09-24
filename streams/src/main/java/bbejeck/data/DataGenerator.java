package bbejeck.data;

import bbejeck.chapter_4.avro.ProductTransaction;
import bbejeck.chapter_4.avro.StockTransaction;
import bbejeck.chapter_6.proto.PurchasedItem;
import bbejeck.chapter_6.proto.RetailPurchase;
import bbejeck.chapter_6.proto.Sensor;
import bbejeck.chapter_7.proto.CoffeePurchase;
import bbejeck.chapter_7.proto.Transaction;
import bbejeck.chapter_8.proto.ClickEvent;
import bbejeck.chapter_8.proto.Employee;
import bbejeck.chapter_8.proto.SensorInfo;
import bbejeck.chapter_8.proto.StockAlert;
import bbejeck.chapter_8.proto.User;
import com.google.protobuf.AbstractMessage;
import net.datafaker.Faker;
import net.datafaker.providers.base.Address;
import net.datafaker.providers.base.Bool;
import net.datafaker.providers.base.Business;
import net.datafaker.providers.base.Commerce;
import net.datafaker.providers.base.Company;
import net.datafaker.providers.base.IdNumber;
import net.datafaker.providers.base.Number;
import net.datafaker.providers.base.Shakespeare;
import net.datafaker.providers.base.Stock;
import net.datafaker.providers.entertainment.ChuckNorris;
import net.datafaker.providers.entertainment.HarryPotter;
import net.datafaker.providers.entertainment.LordOfTheRings;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Utility class that provides various methods for generating data
 * used in examples throughout the book
 */
public class DataGenerator {

    private static final Random random = new Random();
    private static List<String> textileTickers = List.of("PXLW", "CTHR", "WIX");
    private static List<String> techTickers = List.of("DDR", "TK", "OSK");

    public record StockNums(double sharePrice, int num){}
    public record NameScore(String name, double score){}

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
            int whichFaker = number.numberBetween(1, 5);
            switch (whichFaker) {
                case 1 -> text.add(shakespeare.hamletQuote());
                case 2 -> text.add(shakespeare.asYouLikeItQuote());
                case 3 -> text.add(company.bs());
                case 4 -> text.add(chuckNorris.fact());
            }
        }
        return text;
    }

    public static Collection<NameScore> generateFixedNamesWithAScore() {
        final List<String> names = List.of("Anna", "Matthias", "Neil", "Dave", "Rick", "Yeva");
        Number fakeNumer = new Faker().number();
       return names.stream().map(name -> {
            double score = fakeNumer.randomDouble(2, 5, 350);
            return new NameScore(name, score);
        }).toList();
    }

    public static Collection<StockAlert> generateStockAlertsForKTableAggregateExample() {
        Faker faker = new Faker();
        int counter = 0;
        final List<StockAlert> stockAlerts = new ArrayList<>();
        Number number = faker.number();
        String textiles = "textiles";
        String technology = "technology";
        StockAlert.Builder stockAlertBuilder = StockAlert.newBuilder();
        while (counter++ < 2) {

            List<StockAlert> textileStocks = textileTickers.stream()
                    .map(s -> stockAlertBuilder.setSharePrice(number.randomDouble(2, 1, 5))
                            .setShareVolume(number.numberBetween(1, 10))
                            .setMarketSegment(textiles)
                            .setSymbol(s).build()).toList();

            stockAlerts.addAll(textileStocks);
            List<StockAlert> techStocks = techTickers.stream()
                    .map(s -> stockAlertBuilder.setSharePrice(number.randomDouble(2, 1, 5))
                            .setShareVolume(number.numberBetween(1, 10))
                            .setMarketSegment(technology)
                            .setSymbol(s).build()).toList();

            stockAlerts.addAll(techStocks);
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
            int customOrder = random.nextInt(4);
            builder.setCustomerName(customOrder == 0 ? "CUSTOM" : company.name())
                    .setPrice(Double.parseDouble(commerce.price()))
                    .setProductName(commerce.productName())
                    .setTimestamp(instant.plusSeconds(5L * counter).truncatedTo(ChronoUnit.SECONDS).toEpochMilli())
                    .setQuantity(number.numberBetween(1, 1000));
            transactions.add(builder.build());
        }
        return transactions;
    }

    public static Collection<Transaction> generateStockTransactions(int numberRecords) {
        Faker faker = new Faker();
        int counter = 0;
        final List<Transaction> transactions = new ArrayList<>();
        Commerce commerce = faker.commerce();
        Stock stock = faker.stock();
        Number number = faker.number();
        Bool bool = faker.bool();
        Transaction.Builder builder = Transaction.newBuilder();
        Instant instant = Instant.now();

        while (counter++ < numberRecords) {
            builder.setSymbol(stock.nsdqSymbol())
                    .setSharePrice(Double.parseDouble(commerce.price()))
                    .setNumberShares(number.numberBetween(100, 5000))
                    .setTimestamp(instant.plusSeconds(5L * counter).truncatedTo(ChronoUnit.SECONDS).toEpochMilli())
                    .setIsPurchase(bool.bool());
            transactions.add(builder.build());
        }
        return transactions;
    }

    public static Map<String, List<Sensor>> generateSensorReadings(int numberRecords) {
        Faker faker = new Faker();
        final List<String> idNumbers = new ArrayList<>();
        IdNumber idNumber = faker.idNumber();
        for (int i = 0; i < 20; i++) {
            idNumbers.add(idNumber.invalid());
        }
        int numberPerTopic = numberRecords / 3;

        Number number = faker.number();
        Sensor.Builder sensorBuilder = Sensor.newBuilder();
        List<Sensor.Type> types = List.of(Sensor.Type.TEMPERATURE, Sensor.Type.PROXIMITY);
        Map<String, List<Sensor>> recordsMap = new HashMap<>();
        List<String> sensorTopics = List.of("combined-sensors", "temperature-sensors", "proximity-sensors");
        sensorTopics.forEach(sensorTopic -> recordsMap.put(sensorTopic, new ArrayList<>()));
        sensorTopics.forEach(sensorTopic -> {
            List<Sensor> sensorList = recordsMap.get(sensorTopic);
            for (int i = 0; i < numberPerTopic; i++) {
                sensorBuilder.setReading(number.randomDouble(2, 1, 1000));
                sensorBuilder.setId(idNumbers.get(random.nextInt(20)));
                Sensor.Type type = switch (sensorTopic) {
                    case "combined-sensors" -> types.get(number.numberBetween(0, 2));
                    case "temperature-sensors" -> Sensor.Type.TEMPERATURE;
                    case "proximity-sensors" -> Sensor.Type.PROXIMITY;
                    default -> types.get(number.numberBetween(0, 2));
                };
                sensorBuilder.setSensorType(type);
                sensorList.add(sensorBuilder.build());
                sensorBuilder.clear();
            }
        });

        return recordsMap;
    }

    public static Map<String, Collection<? extends AbstractMessage>> generateJoinExampleData(final int numberRecords){
        final Collection<RetailPurchase> purchases = generatePurchasedItems(numberRecords);
        Faker faker = new Faker();
        Number numberFaker = faker.number();
        Instant instant = Instant.now();
        final List<String> drinks = List.of("mocha", "iced-mocha", "brewed coffee", "americano", "espresso");
        final List<String> sizes = List.of("small", "medium", "large", "I need to wake up");
        List<String> customerIds = purchases.stream().map(RetailPurchase::getCustomerId).toList();
        CoffeePurchase.Builder builder = CoffeePurchase.newBuilder();
        final List<CoffeePurchase> coffeePurchases = customerIds.stream().map(cid -> builder.setCustomerId(cid)
                 .setPrice(numberFaker.randomDouble(2, 3, 7))
                 .setPurchaseDate(instant.plusSeconds(numberFaker.randomNumber()).truncatedTo(ChronoUnit.SECONDS).toEpochMilli())
                 .setDrink(drinks.get(numberFaker.numberBetween(0, drinks.size())))
                 .setSize((sizes.get(numberFaker.numberBetween(0, sizes.size())))).build()).toList();


        return Map.of("coffee", coffeePurchases, "retail", purchases);
    }

    public static Collection<RetailPurchase> generatePurchasedItems(final int numberRecords) {
        int counter = 0;
        int maxNumberEmployees = 10;
        int employeeCounter = 0;
        final List<RetailPurchase> retailPurchases = new ArrayList<>();
        Faker faker = new Faker();
        Commerce commerce = faker.commerce();
        Number number = faker.number();
        Business business = faker.business();
        IdNumber idNumber = faker.idNumber();
        Address address = faker.address();
        Instant instant = Instant.now();

        PurchasedItem.Builder purchaseItemBuilder = PurchasedItem.newBuilder();
        RetailPurchase.Builder retailBuilder = RetailPurchase.newBuilder();
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
            retailBuilder.setEmployeeId(Integer.toString(employeeCounter++ % maxNumberEmployees));
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

    public static Collection<User> generateUsers(int num) {
        Faker faker = new Faker();
        User.Builder builder = User.newBuilder();
        int[] ids = IntStream.range(0, num).toArray();
        return Arrays.stream(ids).mapToObj(id -> {
            builder.setAddress(faker.address().fullAddress());
            builder.setId(id);
            builder.setAge(faker.number().numberBetween(20, 65));
            builder.setName(faker.name().fullName());
            return builder.build();
        }).toList();
    }

    public static Collection<ClickEvent> generateClickEvents(int numberUsers, int numEvents) {
        Faker faker = new Faker();
        ClickEvent.Builder builder = ClickEvent.newBuilder();
        AtomicInteger counter = new AtomicInteger(0);
        return Stream.generate(()->{
               builder.setUrl(faker.company().url());
               builder.setUserId(counter.getAndIncrement() % numberUsers);
               builder.setTime(Instant.now().toEpochMilli());
               return builder.build();
        }).limit(numEvents).toList();
    }

    public static Collection<Employee> generateEmployees(int numberEmployees) {
        Faker faker = new Faker();
        Employee.Builder builder = Employee.newBuilder();
        AtomicInteger employeeIdCounter = new AtomicInteger(0);
        return Stream.generate(() -> {
            builder.setAddress(faker.address().fullAddress());
            builder.setId(Integer.toString(employeeIdCounter.getAndIncrement()));
            builder.setDepartment(faker.commerce().department());
            builder.setName(faker.name().fullName());
            builder.setYearsEmployed(faker.number().numberBetween(1,16));
            return builder.build();
        }).limit(numberEmployees).toList();
    }

    public static Collection<Sensor> generateSensorReadingsList(int numberReadings) {
        Sensor.Builder sensorBuilder = Sensor.newBuilder();
        List<Sensor.Type> types = List.of(Sensor.Type.TEMPERATURE, Sensor.Type.PROXIMITY);
        Faker faker = new Faker();
       return Stream.generate(() -> {
            sensorBuilder.setSensorType(types.get(faker.number().numberBetween(0,2)));
            sensorBuilder.setReading(faker.number().randomDouble(2,1,300));
            sensorBuilder.setId(Integer.toString(faker.number().numberBetween(0,11)));
            return sensorBuilder.build();
        }).limit(numberReadings).toList();
    }

    public static Collection<SensorInfo> generateSensorInfo(int numberInfoObjects) {
        SensorInfo.Builder sensorInfoBuilder = SensorInfo.newBuilder();
        int maxIds = 10;
        AtomicInteger idCounter = new AtomicInteger(0);
        Faker faker = new Faker();
        return Stream.generate(() -> {
            sensorInfoBuilder.setId(Integer.toString(idCounter.getAndIncrement() % maxIds));
            sensorInfoBuilder.setGeneration(faker.number().numberBetween(0,4));
            sensorInfoBuilder.setLatlong(faker.address().latitude()+":"+faker.address().longitude());
            sensorInfoBuilder.setDeployed(faker.date().past(1825, TimeUnit.DAYS).getTime());
            return sensorInfoBuilder.build();
        }).limit(numberInfoObjects).toList();
    }



    public static List<String> getLordOfTheRingsCharacters(int number) {
        LordOfTheRings lordOfTheRings = new Faker().lordOfTheRings();
        return Stream.generate(lordOfTheRings::character).limit(number).toList();
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
        DataGenerator.generatePurchasedItems(30).forEach(System.out::println);
        DataGenerator.generateRandomText().forEach(System.out::println);
        DataGenerator.generateSensorReadings(10).entrySet().forEach(System.out::println);
        DataGenerator.generateStockAlertsForKTableAggregateExample().forEach(System.out::println);
        DataGenerator.getLordOfTheRingsCharacters(10).forEach(System.out::println);
        DataGenerator.generateUsers(10).forEach(System.out::println);
        DataGenerator.generateClickEvents(10, 20).forEach(System.out::println);
        DataGenerator.generateSensorInfo(11).forEach(System.out::println);
    }
}

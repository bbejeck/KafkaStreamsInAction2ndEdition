package bbejeck.data;

import bbejeck.chapter_4.avro.ProductTransaction;
import bbejeck.chapter_4.avro.StockTransaction;
import bbejeck.chapter_4.proto.EventsProto;
import com.github.javafaker.Commerce;
import com.github.javafaker.Company;
import com.github.javafaker.Faker;
import com.github.javafaker.HarryPotter;
import com.github.javafaker.LordOfTheRings;
import com.github.javafaker.Number;
import com.github.javafaker.Stock;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

/**
 * User: Bill Bejeck
 * Date: 1/16/21
 * Time: 3:48 PM
 */
public class DataGenerator {


    private DataGenerator() {
    }

    public static Collection<EventsProto.Events> generateEvents(int numberEvents) {
        //TODO this needs to be completed in the meantime data will be hardcoded for events
        Faker faker = new Faker();
        int counter = 0;
        Instant instant = Instant.now();
        Commerce commerce = faker.commerce();
        List<String> users = getLordOfTheRingsCharacters(5);
        final List<EventsProto.Events> events = new ArrayList<>();
        EventsProto.Events.Builder builder = EventsProto.Events.newBuilder();
        int whichEvent = 0;
        while (counter++ < numberEvents) {
             if (whichEvent == 0) {
                 //Login even
                 
             } else if (whichEvent == 1 || whichEvent == 2) {
                 // Search
             } else {
                 // Purchase
             }

           whichEvent +=1;
           if (whichEvent > 2) {
               whichEvent = 0;
           }
        }
        return events;
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


    public static Collection<StockTransaction> generateStockTransaction(int numberRecords) {
        int counter = 0;
        final List<StockTransaction> transactions = new ArrayList<>();
        List<String> symbols = getSymbols(10);
        List<String> brokers = getLordOfTheRingsCharacters(5);
        List<String> customers = getHarryPotterCharacters(10);
        StockTransaction.Builder builder = StockTransaction.newBuilder();
        Random random = new Random();
        Instant instant = Instant.now();
        while(counter++ < numberRecords) {
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
    }
}

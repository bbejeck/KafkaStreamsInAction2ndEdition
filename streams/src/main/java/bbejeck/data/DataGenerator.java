package bbejeck.data;

import bbejeck.chapter_4.avro.ProductTransaction;
import bbejeck.chapter_4.avro.StockTransaction;
import bbejeck.chapter_6.proto.PurchasedItemProto;
import bbejeck.chapter_6.proto.RetailPurchaseProto;
import com.github.javafaker.Address;
import com.github.javafaker.Business;
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
import java.util.List;
import java.util.Random;

/**
 * Utility class that provides various methods for generating data
 * used in examples throughout the book
 */
public class DataGenerator {


    private DataGenerator() {
    }

    public static Collection<String> generateRandomText() {
        Faker faker = new Faker();
        Shakespeare shakespeare = faker.shakespeare();
        Company company = faker.company();
        Number number = faker.number();
        Collection<String> text = new ArrayList<>();
        int randomTextCount = 10;
        for (int i = 0; i < randomTextCount; i++) {
            int whichFaker = number.numberBetween(1, 4);
            switch (whichFaker) {
                case 1 -> text.add(shakespeare.hamletQuote());
                case 2 -> text.add(shakespeare.asYouLikeItQuote());
                case 3 -> text.add(company.bs());
            }
        }
        return text;
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
        int numberOfPurchasedItems = number.numberBetween(1, 4);

        PurchasedItemProto.PurchasedItem.Builder purchaseItemBuilder = PurchasedItemProto.PurchasedItem.newBuilder();
        RetailPurchaseProto.RetailPurchase.Builder retailBuilder = RetailPurchaseProto.RetailPurchase.newBuilder();
        while (counter++ < numberRecords) {
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
            retailBuilder.setDepartment(commerce.department());
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
        DataGenerator.generatePurchasedItems(10).forEach(System.out::println);
        DataGenerator.generateRandomText().forEach(System.out::println);
    }
}

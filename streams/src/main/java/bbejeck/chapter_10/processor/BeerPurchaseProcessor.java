package bbejeck.chapter_10.processor;


import bbejeck.chapter_9.proto.BeerPurchase;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;

import java.text.DecimalFormat;
import java.util.Map;


public class BeerPurchaseProcessor extends ContextualProcessor<String, BeerPurchase, String, BeerPurchase> {

    private final String domesticSalesSink;
    private final String internationalSalesSink;
    private final Map<String, Double> conversionRates;

    public BeerPurchaseProcessor(final String domesticSalesSink,
                                 final String internationalSalesSink,
                                 final Map<String,Double> conversionRates) {
        this.domesticSalesSink = domesticSalesSink;
        this.internationalSalesSink = internationalSalesSink;
        this.conversionRates = conversionRates;
    }

    @Override
    public void process(Record<String, BeerPurchase> beerPurchaseRecord) {
        BeerPurchase beerPurchase = beerPurchaseRecord.value();
        String key  = beerPurchaseRecord.key();
        BeerPurchase.Currency transactionCurrency = beerPurchase.getCurrency();
        if (transactionCurrency != BeerPurchase.Currency.DOLLAR) {
            BeerPurchase.Builder builder = BeerPurchase.newBuilder(beerPurchase);
            double internationalSaleAmount = beerPurchase.getTotalSale();
            String pattern = "###.##";
            DecimalFormat decimalFormat = new DecimalFormat(pattern);
            builder.setCurrency(BeerPurchase.Currency.DOLLAR);
            builder.setTotalSale(Double.parseDouble(decimalFormat.format(convertToDollars(transactionCurrency.name(),internationalSaleAmount))));
            Record<String, BeerPurchase> convertedBeerPurchaseRecord = new Record<>(key,builder.build(), beerPurchaseRecord.timestamp());
            context().forward(convertedBeerPurchaseRecord, internationalSalesSink);
        } else {
            context().forward(beerPurchaseRecord, domesticSalesSink);
        }
    }

    public double convertToDollars(String currentCurrency, double internationalAmount) {
        return internationalAmount / conversionRates.getOrDefault(currentCurrency, 1.0);
    }
}

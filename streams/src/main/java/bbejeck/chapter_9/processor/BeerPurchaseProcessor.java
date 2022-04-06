package bbejeck.chapter_9.processor;


import bbejeck.chapter_9.proto.BearPurchaseProto.BeerPurchase;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;

import java.text.DecimalFormat;
import java.util.Map;


public class BeerPurchaseProcessor extends ContextualProcessor<String, BeerPurchase, String, BeerPurchase> {

    private final String domesticSalesNode;
    private final String internationalSalesNode;
    private final Map<String, Double> conversionRates;

    public BeerPurchaseProcessor(final String domesticSalesNode,
                                 final String internationalSalesNode,
                                 final Map<String,Double> conversionRates) {
        this.domesticSalesNode = domesticSalesNode;
        this.internationalSalesNode = internationalSalesNode;
        this.conversionRates = conversionRates;
    }

    @Override
    public void process(Record<String, BeerPurchase> beerPurchaseRecord) {
        BeerPurchase beerPurchase = beerPurchaseRecord.value();
        String key  = beerPurchaseRecord.key();
        BeerPurchase.Currency transactionCurrency = beerPurchase.getCurrency();
        if (transactionCurrency != BeerPurchase.Currency.DOLLAR) {
            BeerPurchase dollarBeerPurchase;
            BeerPurchase.Builder builder = BeerPurchase.newBuilder(beerPurchase);
            double internationalSaleAmount = beerPurchase.getTotalSale();
            String pattern = "###.##";
            DecimalFormat decimalFormat = new DecimalFormat(pattern);
            builder.setCurrency(BeerPurchase.Currency.DOLLAR);
            builder.setTotalSale(Double.parseDouble(decimalFormat.format(convertToDollars(transactionCurrency.name(),internationalSaleAmount))));
            dollarBeerPurchase = builder.build();
            Record<String, BeerPurchase> convertedBeerPurchaseRecord = new Record<>(key,dollarBeerPurchase, beerPurchaseRecord.timestamp());
            context().forward(convertedBeerPurchaseRecord, internationalSalesNode);
        } else {
            context().forward(beerPurchaseRecord, domesticSalesNode);
        }
    }

    public double convertToDollars(String currentCurrency, double internationalAmount) {
        return internationalAmount / conversionRates.getOrDefault(currentCurrency, 1.0);
    }
}

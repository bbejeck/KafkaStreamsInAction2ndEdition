package bbejeck.chapter_7.aggregator;

import bbejeck.chapter_7.proto.Aggregate;
import bbejeck.chapter_7.proto.Transaction;
import org.apache.kafka.streams.kstream.Aggregator;

/**
 * The {@link Aggregator} used for the {@link bbejeck.chapter_7.StreamsStockTransactionAggregations} example
 */
public class StockAggregator implements Aggregator<String, Transaction, Aggregate> {

    @Override
    public Aggregate apply(String key,
                                               Transaction transaction,
                                               Aggregate aggregate) {

        Aggregate.Builder currAggregate = aggregate.toBuilder();
        currAggregate.setSymbol(key);
        double transactionDollars = transaction.getNumberShares() * transaction.getSharePrice();

        if (transaction.getIsPurchase()) {
            long currentPurchaseVolume = currAggregate.getPurchaseShareVolume();
            currAggregate.setPurchaseShareVolume(currentPurchaseVolume + transaction.getNumberShares());
            double currentPurchaseDollars = currAggregate.getPurchaseDollarAmount();
            currAggregate.setPurchaseDollarAmount(currentPurchaseDollars + transactionDollars);
        } else {
            long currentSalesVolume = currAggregate.getSalesShareVolume();
            currAggregate.setSalesShareVolume(currentSalesVolume + transaction.getNumberShares());
            double currentSalesDollars = currAggregate.getSalesDollarAmount();
            currAggregate.setSalesDollarAmount(currentSalesDollars + transactionDollars);
        }

        double txnSharePrice = transaction.getSharePrice();
        if (currAggregate.getLowestPrice() == 0.0d &&
                currAggregate.getHighestPrice() == 0.0d) {
            currAggregate.setHighestPrice(txnSharePrice);
            currAggregate.setLowestPrice(txnSharePrice);
        } else if (txnSharePrice < currAggregate.getLowestPrice()) {
            currAggregate.setLowestPrice(transaction.getSharePrice());
        } else if (txnSharePrice > currAggregate.getHighestPrice()) {
            currAggregate.setHighestPrice(txnSharePrice);
        }
        return currAggregate.build();
    }
}

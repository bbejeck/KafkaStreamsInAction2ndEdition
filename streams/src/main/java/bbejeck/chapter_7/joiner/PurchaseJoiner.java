package bbejeck.chapter_7.joiner;

import bbejeck.chapter_6.proto.RetailPurchaseProto;
import bbejeck.chapter_7.proto.CoffeePurchaseProto;
import bbejeck.chapter_8.proto.PromotionProto;
import org.apache.kafka.streams.kstream.ValueJoiner;

/**
 * {@link ValueJoiner} used for {@link bbejeck.chapter_7.KafkaStreamsJoinsApp} this will only work for
 * inner joins.  To use this ValueJoiner for left-outer an outer-joins you'll need to add
 * null checks on the two value objects
 */
public class PurchaseJoiner implements ValueJoiner<CoffeePurchaseProto.CoffeePurchase,
        RetailPurchaseProto.RetailPurchase,
        PromotionProto.Promotion> {

    @Override
    public PromotionProto.Promotion apply(final CoffeePurchaseProto.CoffeePurchase coffeePurchase,
                                          final RetailPurchaseProto.RetailPurchase retailPurchase) {
        double coffeeSpend = coffeePurchase.getPrice();
        double storeSpend = retailPurchase.getPurchasedItemsList()
                .stream()
                .mapToDouble(pi -> pi.getPrice() * pi.getQuantity()).sum();
        double promotionPoints = coffeeSpend + storeSpend;
        if (storeSpend > 50.00) {
            promotionPoints += 50.00;
        }
        return PromotionProto.Promotion.newBuilder()
                .setCustomerId(retailPurchase.getCustomerId())
                .setDrink(coffeePurchase.getDrink())
                .setItemsPurchased(retailPurchase.getPurchasedItemsCount())
                .setPoints(promotionPoints).build();
    }
}

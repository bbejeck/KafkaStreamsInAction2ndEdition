package bbejeck.chapter_14;

/**
 * Record class to represent a currency transaction
 */
public record CurrencyExchangeTransaction(double amount, Currency currency) {
    enum Currency {
        EURO(1.05272) ,
        GBP(1.22627),
        JPY(0.00726015),
        CAD(0.733456),
        MXN(0.050),
        USD(1.00);
        private final double exchangeRate;

        Currency(double exchangeRate) {
            this.exchangeRate = exchangeRate;
        }

        double exchangeToDollars(double amount) {
            return amount * exchangeRate;
        }
    }
}

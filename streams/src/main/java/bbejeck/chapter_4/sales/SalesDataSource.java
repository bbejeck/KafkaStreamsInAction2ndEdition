package bbejeck.chapter_4.sales;

import bbejeck.chapter_4.avro.ProductTransaction;
import bbejeck.data.DataGenerator;
import bbejeck.data.DataSource;

import java.util.Collection;

/**
 * Class used for data generation in {@link SalesProduceConsumeApplication}
 */
public class SalesDataSource implements DataSource<ProductTransaction> {
    private final int numberRecords;

    public SalesDataSource(int numberRecords) {
        this.numberRecords = numberRecords;
    }

    public SalesDataSource() {
        this(10);
    }

    @Override
    public Collection<ProductTransaction> fetch() {
        return  DataGenerator.generateProductTransactions(numberRecords);
    }
}

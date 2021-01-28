package bbejeck.chapter_4.sales;

import bbejeck.chapter_4.avro.ProductTransaction;
import bbejeck.common.DataSource;
import bbejeck.utils.DataGenerator;

import java.util.Collection;

/**
 * User: Bill Bejeck
 * Date: 1/27/21
 * Time: 8:04 PM
 */
public class SalesDataSource implements DataSource<ProductTransaction> {
    private int numberRecords = 10;

    public SalesDataSource(int numberRecords) {
        this.numberRecords = numberRecords;
    }

    public SalesDataSource() {
    }

    @Override
    public Collection<ProductTransaction> fetch() {
        return  DataGenerator.generateProductTransactions(10);
    }
}

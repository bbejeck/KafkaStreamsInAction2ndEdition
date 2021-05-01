package bbejeck.data;

import java.util.Collection;

/**
 * Interface for providing a data source for examples
 */
public interface DataSource <T> {

    Collection<T> fetch();
}

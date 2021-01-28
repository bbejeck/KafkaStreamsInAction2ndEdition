package bbejeck.common;

import java.util.Collection;

/**
 * User: Bill Bejeck
 * Date: 1/26/21
 * Time: 7:32 PM
 */
public interface DataSource <T> {

    Collection<T> fetch();
}

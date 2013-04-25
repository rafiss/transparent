package transparent.core.database;

import java.util.Iterator;
import java.util.Map.Entry;

import transparent.core.Module;
import transparent.core.ProductID;

/**
 * An abstract interface for data persistence.
 * 
 * NOTE: All implementations must be thread-safe.
 */
public interface Database
{
	/**
	 * Adds the list of module product IDs to the database associated
	 * with the given module. If any of the product IDs already exist
	 * for that module, then they are not added.
	 */
	public boolean addProductIds(Module module, String... moduleProductIds);

	public Iterator<ProductID> getProductIds(Module module);

	@SuppressWarnings("unchecked") /* needed to suppress varargs warning */
	public boolean addProductInfo(Module module,
			ProductID moduleProductId,
			Entry<String, String>... keyValues);

	public String getMetadata(String key);
	public boolean setMetadata(String key, String value);

	/* TODO: add API for querying (what should it return?) */

	/* TODO: add API for deleting (both metadata and non-metadata) */

    public void close();
}

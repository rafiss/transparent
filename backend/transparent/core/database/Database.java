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
	public boolean addProductIds(Module module, String... moduleProductIds);
	
	public Iterator<ProductID> getProductIds(Module module);

	@SuppressWarnings("unchecked") /* needed to suppress varargs warning */
	public boolean addProductInfo(Module module,
			ProductID moduleProductId,
			Entry<String, String>... keyValues);

	public String getMetadata(String key);
	public boolean setMetadata(String key, String value);

	/* TODO: add api for querying (what should it return?) */

    public void close();
}

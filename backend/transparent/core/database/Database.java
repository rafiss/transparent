package transparent.core.database;

import java.util.Iterator;

import transparent.core.Module;

/**
 * An abstract interface for data persistence.
 * 
 * NOTE: All implementations must be thread-safe.
 */
public interface Database
{
	public boolean addProductIds(Module module, String[] productIds);
	
	public Iterator<String> getProductIds(Module module);
	
	public boolean addProductInfo(Module module,
			String productId, String[] keys, String[] values);

    public void close();
}

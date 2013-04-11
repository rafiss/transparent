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
	public boolean addProductIds(Module module, String[] moduleProductIds);
	
	public Iterator<String> getProductIds(Module module);
	
	public boolean addProductInfo(Module module, String moduleProductId,
			long productId, String[] keys, String[] values);

	public long getMetadataLong(String key);
	public boolean setMetadata(String key, long value);

	/* TODO: add api for querying (what should it return?) */

    public void close();
}

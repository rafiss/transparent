package transparent.core.database;

import transparent.core.Module;
import transparent.core.ProductID;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

/**
 * An abstract interface for data persistence.
 * <p/>
 * NOTE: All implementations must be thread-safe.
 */
public interface Database {
	/**
	 * Adds the list of module product IDs to the database associated
	 * with the given module. If any of the product IDs already exist
	 * for that module, then they are not added.
	 */
	public boolean addProductIds(Module module, String... moduleProductIds);

	public ResultsIterator<ProductID> getProductIds(Module module);

	@SuppressWarnings("unchecked") /* needed to suppress varargs warning */
	public boolean addProductInfo(Module module,
								  ProductID moduleProductId,
								  Entry<String, Object>... keyValues);

	public String getMetadata(String key);

	public boolean setMetadata(String key, String value);

	public Results query(String query,
						 String[] select,
						 String[] whereClause,
						 Relation[] whereRelation,
						 Object[] whereArgs,
						 String groupBy,
                         String orderBy,
						 boolean orderAsc,
						 Integer startRow,
						 Integer rowCount);

	public boolean isReservedKey(String key);

	/* TODO: add API for deleting (both metadata and non-metadata) */

    public void close();

    public interface Results {
        public String getString(int columnIndex);

        public long getLong(int columnIndex);

        public Object get(int columnIndex);

        public boolean hasNext();

        public boolean next();
    }

    public interface ResultsIterator<T> extends Iterator<T> {
        public boolean seekRelative(int position);
    }

	public static enum Type {
		NUMBER,
		STRING;

		public void save(DataOutputStream out) throws IOException {
			switch (this) {
			case NUMBER:
				out.writeByte(0);
				break;
			case STRING:
				out.writeByte(1);
				break;
			default:
				throw new IllegalStateException("Unable to parse type.");
			}
		}

		public static Type load(DataInputStream in) throws IOException {
			switch (in.readByte()) {
			case 0:
				return NUMBER;
			case 1:
				return STRING;
			default:
				throw new IOException("Unable to parse type.");
			}
		}
	};

	public static enum Relation {
		EQUALS,
		NOT_EQUALS,
		LESS_THAN,
		GREATER_THAN;

		public static Relation parse(char operator) {
			switch (operator) {
			case '=':
				return EQUALS;
			case '!':
				return NOT_EQUALS;
			case '<':
				return LESS_THAN;
			case '>':
				return GREATER_THAN;
			default:
				return null;
			}
		}

		@Override
		public String toString() {
			switch (this) {
				case EQUALS:
					return "=";
				case NOT_EQUALS:
					return "~=";
				case LESS_THAN:
					return "<";
				case GREATER_THAN:
					return ">";
				default:
					throw new IllegalStateException("Unable to serialize relation.");
			}
		}
	}
}

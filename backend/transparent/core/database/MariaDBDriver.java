package transparent.core.database;

import transparent.core.Console;
import transparent.core.Module;
import transparent.core.ProductID;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

public class MariaDBDriver implements transparent.core.database.Database {

    private static final String CFG_FILE = "transparent/core/database/transparent.cfg";

    private static final String ENTITY_TABLE = "Entity";
	private static final String NAME_INDEX_TABLE = "NameIndex";

	private static final Column ENTITY_ID_COL = new Column("entity_id", Type.NUMBER, true);
	private static final Column MODULE_ID_COL = new Column("module_id", Type.NUMBER, true);
	private static final Column MODULE_PRODUCT_ID_COL = new Column("module_product_id", Type.STRING, true);
	private static final Column GID_COL = new Column("gid", Type.NUMBER, true);
	private static final Column NAME_COL = new Column("name", Type.STRING, true);
	private static final String DYNAMIC_COLS = "dynamic_cols";
	private static final String DATABASE_NAME = "scratch2";

	private static final Map<String, Column> RESERVED_COLUMNS;
	static {
		Map<String, Column> reserved = new HashMap<String, Column>();
		reserved.put(ENTITY_ID_COL.getName(), ENTITY_ID_COL);
		reserved.put(MODULE_ID_COL.getName(), MODULE_ID_COL);
		reserved.put(MODULE_PRODUCT_ID_COL.getName(), MODULE_PRODUCT_ID_COL);
		reserved.put(GID_COL.getName(), GID_COL);
		RESERVED_COLUMNS = Collections.unmodifiableMap(reserved);
	}

	/* NOTE: All static columns are added before dynamic columns */
	private static Map<String, Column> COLUMNS = new LinkedHashMap<String, Column>();
	static {
		COLUMNS.put(ENTITY_ID_COL.getName(), ENTITY_ID_COL);
		COLUMNS.put(MODULE_ID_COL.getName(), MODULE_ID_COL);
		COLUMNS.put(MODULE_PRODUCT_ID_COL.getName(), MODULE_PRODUCT_ID_COL);
		COLUMNS.put(GID_COL.getName(), GID_COL);
		COLUMNS.put(NAME_COL.getName(), NAME_COL);
	}

    private final Connection connection;

    public MariaDBDriver() throws SQLException, IOException, ClassNotFoundException {
        Properties properties = new Properties();
        properties.load(new FileInputStream(CFG_FILE));

        String host = properties.getProperty("host");
        String username = properties.getProperty("username");
        String password = properties.getProperty("password");
        String driver = properties.getProperty("driver");

        // Register JDBC driver class
		Class.forName(driver);

        Console.println("Connecting to database...");
        connection = DriverManager.getConnection(host, username, password);
        Console.println("Successfully connected to database...");

		loadColumns();
    }

    @Override
    public boolean addProductIds(Module module, String... moduleProductIds) {
        CallableStatement statement = null;
        String query = "{ CALL " + DATABASE_NAME +  ".AddProductId(?, ?) }";

        try {
            statement = connection.prepareCall(query);

            for (String moduleProductId : moduleProductIds) {
                statement.setLong(1, module.getId());
                statement.setString(2, moduleProductId);
                statement.executeUpdate();
            }

            return true;
        } catch (SQLException e) {
            e.printStackTrace();
            module.logError("MariaDBDriver", "addProductIds", "", e);
            return true;
        } finally {
            try {
                if (statement != null) {
                    statement.close();
                }
            } catch (SQLException e) {
                module.logError("MariaDBDriver", "addProductIds", "", e);
            }
        }
    }

    @Override
    public ResultSetIterator getProductIds(Module module) {
        PreparedStatement statement = null;
        try {
            statement = buildSelectStatement(null,
											 new Column[] { ENTITY_ID_COL, MODULE_PRODUCT_ID_COL },
                                             new Column[] { MODULE_ID_COL },
											 new Relation[] { Relation.EQUALS },
                                             new Object[] { module.getId() },
                                             null, null, true, null, null);
            return new ResultSetIterator(module, statement.executeQuery());
        } catch (SQLException e) {
            e.printStackTrace();
            module.logError("MariaDBDriver", "getProductIds", "", e);
            return null;
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    module.logError("MariaDBDriver", "getProductIds", "", e);
                }
            }
        }
    }

	private final boolean addProductInfoHelper(Module module, ProductID productId, Entry<String, Object>... keyValues) {
		PreparedStatement statement = null;
        try {
			Column[] setClause = new Column[keyValues.length];
			Object[] setArgs = new Object[keyValues.length];
            for (int i = 0; i < keyValues.length; i++)
			{
				Entry<String, Object> pair = keyValues[i];
				setClause[i] = COLUMNS.get(pair.getKey());
				if (setClause[i] == null) {
					if (pair.getValue() instanceof String) {
						setClause[i] = new Column(pair.getKey(), Type.STRING, false);
						COLUMNS.put(setClause[i].getName(), setClause[i]);
					} else if (pair.getValue() instanceof Number) {
						setClause[i] = new Column(pair.getKey(), Type.NUMBER, false);
						COLUMNS.put(setClause[i].getName(), setClause[i]);
					} else
						throw new IllegalArgumentException("Unrecognized value type at index "  + i + ".");
				}
				setArgs[i] = pair.getValue();
			}

            statement = buildUpdateStatement(setClause, setArgs,
											 new Column[] { ENTITY_ID_COL },
											 new Relation[] { Relation.EQUALS },
											 new Object[] { productId.getRowId() });
			saveColumns();
            statement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            module.logError("MariaDBDriver", "addProductInfo", "", e);
            return false;
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    module.logError("MariaDBDriver", "addProductInfo", "", e);
                }
            }
        }

        return true;
	}

    @Override
    @SafeVarargs
    public final boolean addProductInfo(Module module,
                                        ProductID productId,
                                        Entry<String, Object>... keyValues)
{
		Column[] setClause = new Column[keyValues.length];
		Object[] setArgs = new Object[keyValues.length];
        for (int i = 0; i < keyValues.length; i++)
		{
			Entry<String, Object> pair = keyValues[i];
			setClause[i] = COLUMNS.get(pair.getKey());
			if (setClause[i] == null) {
				if (pair.getValue() instanceof String) {
					setClause[i] = new Column(pair.getKey(), Type.STRING, false);
					COLUMNS.put(setClause[i].getName(), setClause[i]);
				} else if (pair.getValue() instanceof Number) {
					setClause[i] = new Column(pair.getKey(), Type.NUMBER, false);
					COLUMNS.put(setClause[i].getName(), setClause[i]);
				} else
					throw new IllegalArgumentException("Unrecognized value type at index "  + i + ".");
			}
			setArgs[i] = pair.getValue();
		}

		int dynamicCount = 0;
		int startIndex = 0;
		boolean broken = false;
		ArrayList<Column> setArray = new ArrayList<Column>();
		for (int i = 0; i < setClause.length; i++) {
			if (!setClause[i].isStatic())
				dynamicCount++;
			if (dynamicCount == 3) {
				broken = true;
				Entry<String, Object>[] newKeyValues = Arrays.copyOfRange(keyValues, startIndex, i + 1);
				addProductInfoHelper(module, productId, newKeyValues);
				startIndex = i + 1;
			}
		}
		if (broken) {
			Entry<String, Object>[] newKeyValues = Arrays.copyOfRange(keyValues, startIndex, setClause.length);
			if (newKeyValues.length > 0)
				return addProductInfoHelper(module, productId, newKeyValues);
		}
		return true;
    }

    @Override
    public String getMetadata(String key) {
        PreparedStatement statement = null;

        try {
			String query = "SELECT `meta_value` FROM Metadata WHERE `meta_key`=?";
            statement = connection.prepareStatement(query);
			statement.setString(1, key);
            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getString(1);
            } else {
                return null;
            }
        } catch (SQLException e) {
            Console.printError("MariaDBDriver", "getMetadata", "", e);
            return null;
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    Console.printError("MariaDBDriver", "getMetadata", "", e);
                }
            }
        }
    }

    @Override
    public boolean setMetadata(String key, String value) {
        PreparedStatement statement = null;
        try {
            if (getMetadata(key) != null) {
				String query = "UPDATE Metadata SET `meta_value`=? WHERE `meta_key`=?";
		        statement = connection.prepareStatement(query);
				statement.setString(1, value);
				statement.setString(2, key);
                statement.executeUpdate();
            } else {
				String query = "INSERT INTO Metadata VALUES(?,?)";
		        statement = connection.prepareStatement(query);
				statement.setString(1, key);
				statement.setString(2, value);
                statement.executeUpdate();
            }

            return true;
        } catch (SQLException e) {
            Console.printError("MariaDBDriver", "setMetadata", "", e);
            return false;
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    Console.printError("MariaDBDriver", "setMetadata", "", e);
                }
            }
        }
    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            Console.printError("MariaDBDriver", "close", "", e);
        }
    }

    @Override
    public Results query(String query,
						 String[] select,
						 String[] whereClause,
						 Relation[] whereRelation,
						 Object[] whereArgs,
						 String groupBy,
                         String orderBy,
						 boolean orderAsc,
						 Integer startRow,
						 Integer rowCount)
	{
        PreparedStatement statement = null;
        try {
			Column[] selectColumns = null;
			if (select != null) {
				selectColumns = new Column[select.length];
				for (int i = 0; i < select.length; i++) {
					selectColumns[i] = COLUMNS.get(select[i]);
					if (selectColumns[i] == null) {
						Console.printError("MariaDBDriver", "query", "Unrecognized column name '" + select[i] + "'.");
						return null;
					}
				}
			}

			Column[] whereColumns = null;
			if (whereClause != null) {
				whereColumns = new Column[whereClause.length];
		        for (int i = 0; i < whereClause.length; i++)
				{
					whereColumns[i] = COLUMNS.get(whereClause[i]);
					if (whereColumns[i] == null) {
						Console.printError("MariaDBDriver", "query", "Unrecognized column name '" + whereClause[i] + "'.");
						return null;
					}
				}
			}

			statement = buildSelectStatement(query,
											 selectColumns,
											 whereColumns,
											 whereRelation,
											 whereArgs,
											 COLUMNS.get(groupBy),
											 COLUMNS.get(orderBy), orderAsc,
											 startRow, rowCount);
            return new MariaDBResults(null, statement.executeQuery(), selectColumns);

        } catch (SQLException e) {
            Console.printError("MariaDBDriver", "queryWithAttributes", "", e);
            return null;
        } finally {
            try {
                if (statement != null) {
                    statement.close();
                }
            } catch (SQLException e) {
                Console.printError("MariaDBDriver", "queryWithAttributes", "", e);
            }
        }
    }

    @Override
    public boolean isReservedKey(String key) {
    	return RESERVED_COLUMNS.containsKey(key);
    }

	private void loadColumns() throws IOException
	{
		Integer columnCount = 0;
		try {
			columnCount = Integer.parseInt(getMetadata("column_count"));
		} catch (NumberFormatException e) { }

		String columns = getMetadata("columns");
		if (columns == null)
			return;
		DataInputStream in = new DataInputStream(new ByteArrayInputStream(columns.getBytes()));
		for (int i = 0; i < columnCount; i++) {
			Column col = Column.load(in);
			COLUMNS.put(col.getName(), col);
		}
	}

	private void saveColumns()
	{
		if (!setMetadata("column_count", String.valueOf(COLUMNS.size()))) {
			Console.printError("MariaDBDriver", "saveColumns",
				"Unable to save column count.");
		}

		try {
			ByteArrayOutputStream bytes = new ByteArrayOutputStream();
			DataOutputStream out = new DataOutputStream(bytes);
			for (Column col : COLUMNS.values()) {
					col.save(out);
			}
			out.flush();
			setMetadata("columns", bytes.toString());
		} catch (IOException e) {
			Console.printError("MariaDBDriver", "saveColumns",
				"Unable to save columns.", e);
		}
	}

	private Object checkType(Column clause, Object arg) {
		switch (clause.getType()) {
		case STRING:
			return arg.toString();
		case NUMBER:
			if (arg instanceof String)
				return new BigInteger((String) arg).longValue();
			else return arg;
		default:
			throw new IllegalArgumentException("Unrecognized clause type.");
		}
	}

	private void appendWhereStatement(StringBuilder builder,
									  List<Object> parameters,
									  Column[] whereClause,
									  Relation[] whereRelation,
									  Object[] whereArgs)
	{
		if (whereClause != null && whereClause.length > 0 && whereArgs != null)
		{
	        builder.append(" WHERE ");

			if (whereClause.length != whereArgs.length || whereClause.length != whereRelation.length) {
				throw new IllegalArgumentException("Clauses and arguments must have same length.");
		    }

			for (int i = 0; i < whereClause.length; i++) {
				if (whereArgs[i] instanceof String || whereArgs[i] instanceof Number)
				{
					whereClause[i].appendQueryString(builder, parameters);
					builder.append(whereRelation[i]);
					builder.append("?");
					parameters.add(checkType(whereClause[i], whereArgs[i]));
				}
				else if (whereArgs[i] instanceof String[] || whereArgs[i] instanceof Number[])
				{
					Object[] whereArg = (Object[]) whereArgs[i];

					if (whereArg == null || whereArg.length == 0) {
						throw new IllegalArgumentException("Arrays must be non-empty.");
					}

					whereClause[i].appendQueryString(builder, parameters);
					switch (whereRelation[i]) {
					case EQUALS:
						break;
					case NOT_EQUALS:
						builder.append(" NOT");
						break;
					default:
						throw new IllegalArgumentException("Can only use EQUALS or "
								+ "NOT_EQUALS relations for array values.");
					}
					builder.append(" IN (?");
					parameters.add(checkType(whereClause[i], whereArg[0]));
					for (int j = 1; j < whereArg.length; j++) {
						builder.append(",?");
						parameters.add(checkType(whereClause[i], whereArg[j]));
					}
					builder.append(") ");
				} else {
					throw new IllegalArgumentException("Unknown argument type.");
				}

				if (i != whereClause.length - 1) {
					builder.append(" AND ");
				}
			}
		}
	}

    private PreparedStatement buildSelectStatement(String query,
												   Column[] select,
												   Column[] whereClause,
												   Relation[] whereRelation,
												   Object[] whereArgs,
												   Column groupBy,
                                                   Column orderBy,
												   boolean orderAsc,
												   Integer startRow,
												   Integer rowCount) throws SQLException
	{
        StringBuilder builder = new StringBuilder();
		List<Object> parameters = new ArrayList<Object>();

        builder.append("SELECT ");

		if (select != null && select.length > 0) {
			select[0].appendQueryString(builder, parameters);
		    for (int i = 1; i < select.length; i++) {
		        builder.append(",");
				select[i].appendQueryString(builder, parameters);
		    }
		} else {
			for (Column col : COLUMNS.values()) {
				if (col.isStatic()) {
					col.appendQueryString(builder, parameters);
					builder.append(",");
				} else {
					break;
				}
			}
			builder.append("COLUMN_JSON(");
			builder.append(DYNAMIC_COLS);
			builder.append(")");
		}

        builder.append(" FROM ");
        builder.append(ENTITY_TABLE);

		if (query != null) {
			builder.append(" JOIN (SELECT ");
			builder.append(ENTITY_ID_COL.getName());
			builder.append(" FROM ");
			builder.append(NAME_INDEX_TABLE);
			builder.append(" WHERE query=?) t1 USING (");
			parameters.add(query + ";mode=any");
			builder.append(ENTITY_ID_COL.getName());
			builder.append(")" );
		}

		appendWhereStatement(builder, parameters, whereClause, whereRelation, whereArgs);
		
		if (groupBy != null) {
			builder.append(" GROUP BY ");
			groupBy.appendQueryString(builder, parameters);
		}

        if (orderBy != null) {
			builder.append(" ORDER BY ");
			orderBy.appendQueryString(builder, parameters);

			if (orderAsc) {
				builder.append(" ASC");
			} else {
				builder.append(" DESC");
			}
        }

        if (startRow == null) {
			if (rowCount != null) {
	            builder.append(" LIMIT 1,");
				builder.append(rowCount);
			}
        } else {
			if (rowCount == null) {
	            builder.append(" LIMIT ");
	            builder.append(startRow);
	            builder.append(',');
	            builder.append(1 << 64 - 1);
			} else {
	            builder.append(" LIMIT ");
	            builder.append(startRow);
	            builder.append(',');
	            builder.append(rowCount);
			}
        }

        PreparedStatement statement = connection.prepareStatement(builder.toString());

		int index = 1;
		for (int i = 0; i < parameters.size(); i++) {
			if (parameters.get(i) instanceof String) {
				statement.setString(index++, (String) parameters.get(i));
			} else {
				statement.setLong(index++, ((Number) parameters.get(i)).longValue());
			}
		}

        return statement;
    }

    private PreparedStatement buildUpdateStatement(Column[] setClause,
                                                   Object[] setArgs,
												   Column[] whereClause,
												   Relation[] whereRelation,
                                                   Object[] whereArgs) throws SQLException
	{
        StringBuilder builder = new StringBuilder("UPDATE ");
		List<Object> parameters = new ArrayList<Object>();

        builder.append(ENTITY_TABLE);
        builder.append(" SET ");

		if (setClause == null || setClause.length == 0)
			throw new IllegalArgumentException("Must have at least one set clause.");

		StringBuilder dynamicBuilder = new StringBuilder(DYNAMIC_COLS);
		List<Object> dynamicParameters = new ArrayList<Object>();
		dynamicBuilder.append("=COLUMN_ADD(");
		dynamicBuilder.append(DYNAMIC_COLS);
        for (int i = 0; i < setClause.length; i++) {
			if (setClause[i].isStatic()) {
				setClause[i].appendQueryString(builder, parameters);
				builder.append("=?,");
				parameters.add(checkType(setClause[i], setArgs[i]));
			} else {
	            dynamicBuilder.append(",'");
				dynamicBuilder.append(setClause[i].getName());
	            dynamicBuilder.append("',?");
				dynamicParameters.add(checkType(setClause[i], setArgs[i]));
			}
        }
		dynamicBuilder.append(") ");
		if (dynamicParameters.size() == 0)
			builder.deleteCharAt(builder.length() - 1);
		else {
			builder.append(dynamicBuilder);
			parameters.addAll(dynamicParameters);
		}

		appendWhereStatement(builder, parameters, whereClause, whereRelation, whereArgs);

        PreparedStatement statement = connection.prepareStatement(builder.toString());

		for (int i = 0; i < parameters.size(); i++) {
			if (parameters.get(i) instanceof String) {
				statement.setString(i + 1, (String) parameters.get(i));
			} else if (parameters.get(i) instanceof Number) {
				statement.setLong(i + 1, ((Number) parameters.get(i)).longValue());
			}
		}

        return statement;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException {
        // Test client

        Database database = new MariaDBDriver();

        Module testModule = new Module(1, null, "moduleName", "sourceName", "url", "sourceUrl", null, null, false, false);

        int numInserts = 1000;
        String[] products = new String[numInserts];

        for (int i = 0; i < numInserts; i++) {
            products[i] = "product" + i;
        }

        long start = System.nanoTime();
        database.addProductIds(testModule, products);
        System.err.println(numInserts + " inserts took " + ((System.nanoTime() - start) / 1e6) +
                                   "ms");

        ResultsIterator<ProductID> productIDIterator = database.getProductIds(testModule);

        start = System.nanoTime();
        while (productIDIterator.hasNext()) {
            ProductID productID = productIDIterator.next();

            AbstractMap.SimpleEntry<String, Object> firstEntry =
            		new AbstractMap.SimpleEntry("foo", System.nanoTime());
            AbstractMap.SimpleEntry<String, Object> secondEntry =
					new AbstractMap.SimpleEntry("baz", "grep");
            AbstractMap.SimpleEntry<String, Object> thirdEntry =
					new AbstractMap.SimpleEntry("something", "else");
            AbstractMap.SimpleEntry<String, Object> fourthEntry =
					new AbstractMap.SimpleEntry("name", "item " + System.nanoTime());
            AbstractMap.SimpleEntry<String, Object> fifthEntry =
					new AbstractMap.SimpleEntry("something2", "item " + System.nanoTime());
            AbstractMap.SimpleEntry<String, Object> sixthEntry =
					new AbstractMap.SimpleEntry("something3", "item " + System.nanoTime());
            AbstractMap.SimpleEntry<String, Object> seventhEntry =
					new AbstractMap.SimpleEntry("something4", "item " + System.nanoTime());
            AbstractMap.SimpleEntry<String, Object> eighthEntry =
					new AbstractMap.SimpleEntry("something5", "item " + System.nanoTime());
            database.addProductInfo(testModule, productID, firstEntry, secondEntry, thirdEntry, fourthEntry, fifthEntry, sixthEntry, seventhEntry, eighthEntry);
        }
        System.err.println(numInserts + " updates took "
        		+ ((System.nanoTime() - start) / 4e6) + "ms");
        Results search = database.query("item", null,
                                new String[] { "baz" },
								new Relation[] { Relation.EQUALS },
                                new String[] { "grep" },
                                null, "foo", false, null, 50);

        while (search.next()) {
            System.out.println(search.getLong(1) + "\t" + search.getString(2) + "\t" + search.getString(3)
					+ "\t" + search.getString(4) + "\t" + search.getString(5) + "\t" + search.getString(6));
        }

        System.err.println("First query complete.");

        search = database.query("item", null,
                                new String[] { "baz" },
								new Relation[] { Relation.EQUALS },
                                new String[] { "grep" },
                                null, "something", false, 1, 3);

        while (search.next()) {
            System.out.println(search.getLong(1));
        }

        database.setMetadata("key1", "value1");
        assert database.getMetadata("key1").equals("value1");
        assert database.getMetadata("key2") == null;
        database.setMetadata("key1", "value2");
        assert database.getMetadata("key1").equals("value2");
    }

	private static class Column
	{
		private String name;
		private boolean isStatic;
		private Type type;

		public Column(String name, Type type, boolean isStatic) {
			if (type == null || name == null)
				throw new IllegalArgumentException("Arguments cannot be null.");
			this.name = name;
			this.type = type;
			this.isStatic = isStatic;
		}

		public boolean isStatic() {
			return this.isStatic;
		}

		public Type getType() {
			return this.type;
		}

		public String getName() {
			return this.name;
		}

		public void appendQueryString(StringBuilder builder, List<Object> parameters)
		{
			if (isStatic) {
				builder.append('`');
				builder.append(name);
				builder.append('`');
			} else {
				builder.append("COLUMN_GET(");
				builder.append(DYNAMIC_COLS);
				builder.append(",?");
				parameters.add(name);
				switch (type) {
				case NUMBER:
					builder.append(" AS INT) ");
					break;
				case STRING:
					builder.append(" AS CHAR) ");
					break;
				}
			}
		}

		public void save(DataOutputStream out) throws IOException {
			out.writeByte(isStatic ? 1 : 0);
			type.save(out);
			out.writeUTF(name);
		}

		public static Column load(DataInputStream in) throws IOException {
			boolean isStatic = in.readByte() == 1;
			Type type = Type.load(in);
			String name = in.readUTF();
			return new Column(name, type, isStatic);
		}
	}

	private static class MariaDBResults implements Results {
	    private final Module owner;
	    private final ResultSet resultSet;
	    private final Column[] select;

	    public MariaDBResults(Module owner, ResultSet resultSet, Column[] select) {
	        this.owner = owner;
	        this.resultSet = resultSet;
	        this.select = select;
	    }

	    @Override
	    public String getString(int columnIndex) {
	        try {
	            return resultSet.getString(columnIndex);
	        } catch (SQLException e) {
	            if (owner == null)
	                Console.printError("MariaDBResults", "getString", "", e);
	            else
	                owner.logError("MariaDBResults", "getString", "", e);
	            return null;
	        }
	    }

	    @Override
	    public long getLong(int columnIndex) {
	        try {
	            return resultSet.getLong(columnIndex);
	        } catch (SQLException e) {
	            if (owner == null)
	                Console.printError("MariaDBResults", "getLong", "", e);
	            else
	                owner.logError("MariaDBResults", "getLong", "", e);
	            return 0;
	        }
	    }

	    @Override
	    public Object get(int columnIndex) {
	        try {
	        	switch (select[columnIndex - 1].getType()) {
	        	case STRING:
	            	return resultSet.getString(columnIndex);
	        	case NUMBER:
	        		return resultSet.getLong(columnIndex);
	        	default:
	        		throw new IllegalStateException("Unrecognized column type.");
	        	}
	        } catch (Exception e) {
	            if (owner == null)
	                Console.printError("MariaDBResults", "get", "", e);
	            else
	                owner.logError("MariaDBResults", "get", "", e);
	            return null;
	        }
	    }

	    @Override
	    public boolean hasNext() {
	        try {
	            return !resultSet.isLast();
	        } catch (SQLException e) {
	            if (owner == null)
	                Console.printError("MariaDBResults", "hasNext", "", e);
	            else
	                owner.logError("MariaDBResults", "hasNext", "", e);
	            return false;
	        }
	    }
	
	    @Override
	    public boolean next() {
	        try {
	            return resultSet.next();
	        } catch (SQLException e) {
	            if (owner == null)
	                Console.printError("MariaDBResults", "next", "", e);
	            else
	                owner.logError("MariaDBResults", "next", "", e);
	            return false;
	        }
	    }
	}
}

class ResultSetIterator implements Database.ResultsIterator<ProductID> {

    private final ResultSet resultSet;
    private final Module owner;

    public ResultSetIterator(Module owner, ResultSet resultSet) {
        this.owner = owner;
        this.resultSet = resultSet;
    }

    @Override
    public boolean hasNext() {
        try {
            return !resultSet.isLast();
        } catch (SQLException e) {
            owner.logError("ResultSetIterator", "hasNext", "", e);
            return false;
        }
    }

    @Override
    public ProductID next() {
        try {
            if (resultSet.next()) {
                return new ProductID(resultSet.getLong(1), resultSet.getString(2));
            } else {
                return null;
            }
        } catch (SQLException e) {
            owner.logError("ResultSetIterator", "next", "", e);
            return null;
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Cannot remove from ResultSet.");
    }

    @Override
    public boolean seekRelative(int position) {
        try {
            return resultSet.relative(position);
        } catch (SQLException e) {
            owner.logError("ResultSetIterator", "seekRelative", "", e);
            return false;
        }
    }
}

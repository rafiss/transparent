package transparent.core.database;

import transparent.core.Console;
import transparent.core.Module;
import transparent.core.ProductID;
import transparent.core.database.Database.Results;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;

public class MariaDBDriver implements transparent.core.database.Database {

    private static final String CFG_FILE = "transparent/core/database/transparent.cfg";

    private static final String MODULE_ID = "module_id";

    private static final String METADATA_TABLE = "Metadata";
    private static final String ENTITY_TABLE = "Entity";
    private static final String PROPERTY_TYPE_TABLE = "PropertyType";
    private static final String PROPERTY_TABLE = "Property";
    private static final String TRAIT_TABLE = "Trait";
    private static final String MODEL_NAME = "vModel";

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
    }

    @Override
    public boolean addProductIds(Module module, String... moduleProductIds) {
        CallableStatement statement = null;
        String query = "{ CALL transparent.AddProductId(?, ?, ?) }";

        try {
            connection.setAutoCommit(false);
            statement = connection.prepareCall(query);

            for (String moduleProductId : moduleProductIds) {
                statement.setString(1, module.getIdString());
                statement.setLong(2, module.getId());
                statement.setString(3, moduleProductId);
                statement.executeUpdate();
            }

            connection.commit();
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
            module.logError("MariaDBDriver", "addProductIds", "", e.getMessage());
            return true;
        } finally {
            try {
                connection.setAutoCommit(true);
                if (statement != null) {
                    statement.close();
                }
            } catch (SQLException e) {
                module.logError("MariaDBDriver", "addProductIds", "", e.getMessage());
            }
        }
    }

    @Override
    public Iterator<ProductID> getProductIds(Module module) {
        PreparedStatement statement = null;
        try {
            statement = buildSelectStatement(ENTITY_TABLE,
                                             new String[] { "entity_id", "name" },
                                             new long[] { module.getId() },
                                             "entity_id",
                                             null,
                                             null,
                                             null,
                                             null);
            return new ResultSetIterator(module, statement.executeQuery());
        } catch (SQLException e) {
            e.printStackTrace();
            module.logError("MariaDBDriver", "getProductIds", "", e.getMessage());
            return null;
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    module.logError("MariaDBDriver", "getProductIds", "", e.getMessage());
                }
            }
        }
    }

    @Override
    @SafeVarargs
    public final boolean addProductInfo(Module module,
                                        ProductID productId,
                                        Entry<String, String>... keyValues) {
        CallableStatement statement = null;
        String query = "{ CALL transparent.InsertNewAttribute(?, ?, ?, ?) }";
        int index = 0;

        try {
            connection.setAutoCommit(false);
            statement = connection.prepareCall(query);

            for (Entry<String, String> pair : keyValues) {
                statement.setString(1, module.getIdString());
                statement.setInt(2, productId.getRowId());
                statement.setString(3, pair.getKey());
                statement.setString(4, pair.getValue());
                statement.addBatch();

                if (++index % 1000 == 0) {
                    statement.executeBatch();
                    statement.clearBatch();
                }
            }

            if (index % 1000 != 0) {
                statement.executeBatch();
            }

            connection.commit();
        } catch (SQLException e) {
            module.logError("MariaDBDriver", "addProductInfo", "", e.getMessage());
            return false;
        } finally {
            try {
                connection.setAutoCommit(true);
                if (statement != null) {
                    statement.close();
                }
            } catch (SQLException e) {
                module.logError("MariaDBDriver", "addProductInfo", "", e.getMessage());
            }
        }

        return true;
    }

    @Override
    public String getMetadata(String key) {
        PreparedStatement statement = null;

        try {
            statement = buildSelectStatement(METADATA_TABLE,
                                             new String[] { "meta_value" },
                                             null,
                                             null,
                                             null,
                                             new String[] { "meta_key" },
                                             new String[] { "=" + key },
                                             null);
            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getString(1);
            } else {
                return null;
            }
        } catch (SQLException e) {
            Console.printError("MariaDBDriver", "getMetadata", "", e.getMessage());
            return null;
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    Console.printError("MariaDBDriver", "getMetadata", "", e.getMessage());
                }
            }
        }
    }

    @Override
    public boolean setMetadata(String key, String value) {
        PreparedStatement statement = null;
        try {
            connection.setAutoCommit(false);

            if (getMetadata(key) != null) {
                statement = buildUpdateStatement(METADATA_TABLE,
                                                 new String[] { "meta_value" },
                                                 new String[] { value },
                                                 new String[] { "meta_key" },
                                                 new String[] { key });
                statement.executeUpdate();
            } else {
                statement = buildInsertStatement(METADATA_TABLE,
                                                 new String[] { key, value });
                statement.executeUpdate();
            }

            connection.commit();
            return true;
        } catch (SQLException e) {
            Console.printError("MariaDBDriver", "setMetadata", "", e.getMessage());
            return false;
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                    connection.setAutoCommit(true);
                } catch (SQLException e) {
                    Console.printError("MariaDBDriver", "setMetadata", "", e.getMessage());
                }
            }
        }
    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            Console.printError("MariaDBDriver", "close", "", e.getMessage());
        }
    }

    @Override
    public Results query(ProductID[] rowIds, String[] properties) {
        PreparedStatement statement = null;

        try {
            statement = buildQueryStatement(rowIds, properties);
            return new MariaDBResults(null, statement.executeQuery());
        } catch (SQLException e) {
            Console.printError("MariaDBDriver", "query", "", e.getMessage());
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    Console.printError("MariaDBDriver", "query", "", e.getMessage());
                }
            }
        }

        return null;
    }

    private PreparedStatement buildInsertStatement(String tableName,
                                                   String[] values) throws SQLException {
        StringBuilder stringBuilder = new StringBuilder("INSERT INTO ");

        stringBuilder.append(tableName);
        stringBuilder.append(" VALUES (");

        for (int i = 0; i < values.length; i++) {
            stringBuilder.append("?");
            if (i != values.length - 1) {
                stringBuilder.append(",");
            }
        }

        stringBuilder.append(")");

        PreparedStatement statement = connection.prepareStatement(stringBuilder.toString());

        for (int i = 0; i < values.length; i++) {
            statement.setString(i + 1, values[i]);
        }

        return statement;
    }

    private PreparedStatement buildSelectStatement(String tableName, String[] select,
                                                   long[] moduleIds, String rowIdName,
                                                   ProductID[] rowIds, String[] whereClause,
                                                   String[] whereArgs,
                                                   String orderBy) throws SQLException {
        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append("SELECT ");

        for (int i = 0; i < select.length; i++) {
            stringBuilder.append(select[i]);
            if (i != select.length - 1) {
                stringBuilder.append(",");
            }
        }

        stringBuilder.append(" FROM ");
        stringBuilder.append(tableName);
        stringBuilder.append(" WHERE ");

        if (moduleIds != null) {
            for (long moduleId : moduleIds) {
                stringBuilder.append(MODULE_ID);
                stringBuilder.append("=");
                stringBuilder.append(moduleId);
                stringBuilder.append(" AND ");
            }
        }

        if (rowIds != null) {
            stringBuilder.append(rowIdName);
            stringBuilder.append(" IN (");

            for (ProductID rowId : rowIds) {
                stringBuilder.append(rowId.getRowId());
                stringBuilder.append(",");
            }

            // Delete trailing ","
            stringBuilder.deleteCharAt(stringBuilder.length() - 1);
            stringBuilder.append(") AND ");
        }

        if (whereClause != null) {
            for (int i = 0; i < whereClause.length; i++) {
                stringBuilder.append(whereClause[i]);
                stringBuilder.append(whereArgs[i].charAt(0));
                stringBuilder.append("? AND ");
            }
        }

        // Delete trailing " AND "
        if (stringBuilder.substring(stringBuilder.length() - 5).equals(" AND ")) {
            stringBuilder.delete(stringBuilder.length() - 5, stringBuilder.length() - 1);
        }

        if (orderBy != null) {
            stringBuilder.append(" ORDER BY ");
            stringBuilder.append(orderBy);
            stringBuilder.append(" ASC");
        }

        PreparedStatement statement = connection.prepareStatement(stringBuilder.toString());

        if (whereArgs != null) {
            for (int i = 0; i < whereArgs.length; i++) {
                statement.setString(i + 1, whereArgs[i].substring(1));
            }
        }

        return statement;
    }

    private PreparedStatement buildUpdateStatement(String tableName, String[] setClause,
                                                   String[] setArgs, String[] whereClause,
                                                   String[] whereArgs) throws SQLException {
        StringBuilder stringBuilder = new StringBuilder("UPDATE ");

        stringBuilder.append(tableName);
        stringBuilder.append(" SET ");

        for (int i = 0; i < setClause.length; i++) {
            stringBuilder.append(setClause[i]);
            stringBuilder.append("=?");
            if (i != setClause.length - 1) {
                stringBuilder.append(" AND ");
            }
        }

        stringBuilder.append(" WHERE ");

        for (int i = 0; i < whereClause.length; i++) {
            stringBuilder.append(whereClause[i]);
            stringBuilder.append("=?");
            if (i != setClause.length - 1) {
                stringBuilder.append(" AND ");
            }
        }

        PreparedStatement statement = connection.prepareStatement(stringBuilder.toString());

        for (int i = 0; i < setArgs.length; i++) {
            statement.setString(i + 1, setArgs[i]);
        }

        for (int i = 0; i < whereArgs.length; i++) {
            statement.setString(i + setArgs.length + 1, whereArgs[i]);
        }

        return statement;
    }

    private PreparedStatement buildQueryStatement(ProductID[] rowIds,
                                                  String[] properties) throws SQLException {
        StringBuilder stringBuilder = new StringBuilder("SELECT EntityID,PropertyName,TraitValue FROM ");

        stringBuilder.append(MODEL_NAME);
        stringBuilder.append(" WHERE EntityID IN (");

        for (int i = 0; i < rowIds.length; i++) {
            stringBuilder.append(rowIds[i].getRowId());

            if (i != rowIds.length - 1) {
                stringBuilder.append(",");
            }
        }

        stringBuilder.append(") AND PropertyName IN (");

        for (int i = 0; i < properties.length; i++) {
            stringBuilder.append("?");

            if (i != properties.length - 1) {
                stringBuilder.append(",");
            }
        }

        stringBuilder.append(") ORDER BY EntityID ASC");

        PreparedStatement statement = connection.prepareStatement(stringBuilder.toString());

        for (int i = 0; i < properties.length; i++) {
            statement.setString(i + 1, properties[i]);
        }

        return statement;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException {
        // Test client

        Database database = new MariaDBDriver();

        Module testModule = new Module(1, null, "moduleName", "sourceName", null, false, false);

        int numInserts = 1000;
        String[] products = new String[numInserts];

        for (int i = 0; i < numInserts; i++) {
            products[i] = "product" + i;
        }

        long start = System.nanoTime();
        database.addProductIds(testModule, products);
        System.err.println(numInserts + " inserts took " + ((System.nanoTime() - start) / 1e6) +
                                   "ms");

        Iterator<ProductID> productIDIterator = database.getProductIds(testModule);

        start = System.nanoTime();
        while (productIDIterator.hasNext()) {
            ProductID productID = productIDIterator.next();

            AbstractMap.SimpleEntry<String, String> entry = new AbstractMap.SimpleEntry("foo", "bar");
            database.addProductInfo(testModule, productID, entry);
            entry = new AbstractMap.SimpleEntry("baz", "grep");
            database.addProductInfo(testModule, productID, entry);
            entry = new AbstractMap.SimpleEntry("something", "else");
            database.addProductInfo(testModule, productID, entry);
            entry = new AbstractMap.SimpleEntry("something", "else2");
            database.addProductInfo(testModule, productID, entry);

        }
        System.err.println(numInserts + " updates took " + ((System.nanoTime() - start) / 4e6) +
                                   "ms");


        Results search = database.query(new ProductID[] { new ProductID(2, null),
                                                          new ProductID(3, null) },
                                        new String[] { "foo", "something" });

        while (search.next()) {
            System.out.println(search.getLong(1) + " " + search.getString(2));
        }


        database.setMetadata("key1", "value1");
        assert database.getMetadata("key1").equals("value1");
        assert database.getMetadata("key2") == null;
        database.setMetadata("key1", "value2");
        assert database.getMetadata("key1").equals("value2");
    }
}

class MariaDBResults implements Results {
    private final Module owner;
    private final ResultSet resultSet;

    public MariaDBResults(Module owner, ResultSet resultSet) {
        this.owner = owner;
        this.resultSet = resultSet;
    }

    @Override
    public String getString(int columnIndex) {
        try {
            return resultSet.getString(columnIndex);
        } catch (SQLException e) {
        	if (owner == null)
        		Console.printError("MariaDBResults", "getString", "", e.getMessage());
        	else
        		owner.logError("MariaDBResults", "getString", "", e.getMessage());
            return null;
        }
    }

    @Override
    public long getLong(int columnIndex) {
        try {
            return resultSet.getLong(columnIndex);
        } catch (SQLException e) {
        	if (owner == null)
        		Console.printError("MariaDBResults", "getLong", "", e.getMessage());
        	else
        		owner.logError("MariaDBResults", "getLong", "", e.getMessage());
            return 0;
        }
    }

    @Override
    public boolean hasNext() {
        try {
            return !resultSet.isLast();
        } catch (SQLException e) {
        	if (owner == null)
        		Console.printError("MariaDBResults", "hasNext", "", e.getMessage());
        	else
        		owner.logError("MariaDBResults", "hasNext", "", e.getMessage());
            return false;
        }
    }

    @Override
    public boolean next() {
        try {
            return resultSet.next();
        } catch (SQLException e) {
        	if (owner == null)
        		Console.printError("MariaDBResults", "next", "", e.getMessage());
        	else
        		owner.logError("MariaDBResults", "next", "", e.getMessage());
            return false;
        }
    }
}

class ResultSetIterator implements Iterator<ProductID> {

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
            owner.logError("ResultSetIterator", "hasNext", "", e.getMessage());
            return false;
        }
    }

    @Override
    public ProductID next() {
        try {
            if (resultSet.next()) {
                return new ProductID(resultSet.getInt(1), resultSet.getString(2));
            } else {
                return null;
            }
        } catch (SQLException e) {
            owner.logError("ResultSetIterator", "next", "", e.getMessage());
            return null;
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Cannot remove from ResultSet.");
    }
}
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
import java.util.Map.Entry;
import java.util.Properties;

public class MariaDBDriver implements transparent.core.database.Database {

    private static final String CFG_FILE = "transparent/core/database/transparent.cfg";

    private static final String MODULE_ID = "module_id";
    private static final String METADATA_TABLE = "Metadata";
    private static final String ENTITY_TABLE = "Entity";
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
            module.logError("MariaDBDriver", "addProductIds", "", e);
            return true;
        } finally {
            try {
                connection.setAutoCommit(true);
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
            module.logError("MariaDBDriver", "addProductInfo", "", e);
            return false;
        } finally {
            try {
                connection.setAutoCommit(true);
                if (statement != null) {
                    statement.close();
                }
            } catch (SQLException e) {
                module.logError("MariaDBDriver", "addProductInfo", "", e);
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
            Console.printError("MariaDBDriver", "setMetadata", "", e);
            return false;
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                    connection.setAutoCommit(true);
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
    public Results query(ProductID[] rowIds, String[] properties) {
        PreparedStatement statement = null;

        try {
            statement = buildQueryStatement(rowIds, properties);
            return new MariaDBResults(null, statement.executeQuery());
        } catch (SQLException e) {
            Console.printError("MariaDBDriver", "query", "", e);
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    Console.printError("MariaDBDriver", "query", "", e);
                }
            }
        }

        return null;
    }

    /**
     * Query the database with the given parameters and return a Results object.
     * <br />
     * <br />
     * <b>NOTE: Only the first WHERE clause is applied.</b>
     *
     * @param whereClause Fields names to apply
     * @param whereArgs   Values for the given fields
     * @param sortBy      Value to sort by
     * @param sortAsc     True if sort ascending, false if sort descending
     * @param startRow    The index in the query result to first return
     * @param numRows     The number of rows to return
     * @param onlyIndexes True if only distinct EntityIDs are to be returned
     * @return Results returned from the database
     */
    @Override
    public Results query(String[] whereClause, String[] whereArgs, String sortBy, boolean sortAsc,
                         Integer startRow, Integer numRows, boolean onlyIndexes) {
        CallableStatement statement = null;
        String query = null;

        if (onlyIndexes) {
            query = "{ CALL transparent.QueryWithIndexes(?, ?, ?, ?, ?, ?) }";
        } else {
            query = "{ CALL transparent.QueryWithAttributes(?, ?, ?, ?, ?, ?) }";
        }

        try {
            statement = connection.prepareCall(query);

            statement.setString(1, whereClause[0]);
            statement.setString(2, whereArgs[0]);
            statement.setString(3, sortBy);
            statement.setBoolean(4, sortAsc);

            if (startRow == null) {
                statement.setInt(5, 1);
            } else {
                statement.setInt(5, startRow);
            }

            if (numRows == null) {
                statement.setInt(6, 1 << 64 - 1);
            } else {
                statement.setInt(6, numRows);
            }

            return new MariaDBResults(null, statement.executeQuery());

        } catch (Exception e) {
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

        ResultsIterator<ProductID> productIDIterator = database.getProductIds(testModule);

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

        search = database.query(new String[] { "foo" },
                                new String[] { "bar" },
                                "something",
                                false,
                                null,
                                5,
                                false);

        while (search.next()) {
            System.out.println(search.getLong(1) + "\t" + search.getString(2) + "\t\t\t" + search
                    .getString(3));
        }

        System.err.println("First query complete.");

        search = database.query(new String[] { "foo" },
                                new String[] { "bar" },
                                "something",
                                false,
                                1,
                                3,
                                true);

        while (search.next()) {
            System.out.println(search.getLong(1));
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
                return new ProductID(resultSet.getInt(1), resultSet.getString(2));
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
package transparent.core.database;

import transparent.core.Module;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class MariaDBDriver implements transparent.core.database.Database {

    private static final String CFG_FILE = "transparent/core/database/transparent.cfg";

    private static final String MODULE_NAME_KEY = "module_name";
    private static final String MODULE_SOURCE_KEY = "module_source";
    private static final String PRODUCT_ID_KEY = "product_id";

    private static final String ENTITY_TABLE = "Entity";
    private static final String PROPERTY_TYPE_TABLE = "PropertyType";
    private static final String PROPERTY_TABLE = "Property";
    private static final String MEASUREMENT_TABLE = "Measurement";
    private static final String TRAIT_TABLE = "Trait";

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

        System.err.println("Connecting to database...");
        connection = DriverManager.getConnection(host, username, password);
        System.err.println("Successfully connected to database...");
    }

    @Override
    public boolean addProductIds(Module module, String[] moduleProductIds) {
        // TODO: Batch SQL statements

        try {
            for (String moduleProductId : moduleProductIds) {
                // Insert moduleProductId as Entity
                long generatedEntityKey = insertIntoEntity(moduleProductId).get(0);

                // Insert MODULE_NAME_KEY and MODULE_SOURCE_KEY as PropertyTypes
                long generatedPropertyTypeKey1 = insertIntoPropertyType(MODULE_NAME_KEY,
                                                                        true).get(0);
                long generatedPropertyTypeKey2 = insertIntoPropertyType(MODULE_SOURCE_KEY,
                                                                        true).get(0);

                // Associate EntityIDs with PropertyTypeIds in Property
                long generatedPropertyKey1 = insertIntoProperty(generatedEntityKey,
                                                                generatedPropertyTypeKey1).get(0);
                long generatedPropertyKey2 = insertIntoProperty(generatedEntityKey,
                                                                generatedPropertyTypeKey2).get(0);

                // Update Trait table with correct values
                insertIntoTrait(generatedPropertyKey1, module.getModuleName());
                insertIntoTrait(generatedPropertyKey2, module.getSourceName());
            }
        } catch (SQLException e) {
            System.err.println("MariaDBDriver.addProductIds ERROR:"
                                       + " Exception thrown (" + e.getMessage() + ").");
            return false;
        }

        return true;
    }

    @Override
    public Iterator<String> getProductIds(Module module) {
        Statement statement = null;
        try {
            statement = connection.createStatement();
            statement.execute("SELECT EntityID FROM vModel WHERE PropertyName='" + MODULE_NAME_KEY +
                                      "' AND TraitValue='" + module.getModuleName() +
                                      "' GROUP BY EntityID");
            return new ResultSetIterator(statement.getResultSet());
        } catch (SQLException e) {
            System.err.println("MariaDBDriver.getProductIds ERROR:"
                                       + " Exception thrown (" + e.getMessage() + ").");
            return null;
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    System.err.println("MariaDBDriver.getProductIds ERROR:"
                                               + " Exception thrown (" + e.getMessage() + ").");
                }
            }
        }
    }

    @Override
    public boolean addProductInfo(Module module, String moduleProductId, long productId,
                                  String[] keys, String[] values) {
        assert (keys.length == values.length);

        try {
            // TODO: Add this to the Measurement table without casting?
            insertNewAttribute(moduleProductId, PRODUCT_ID_KEY, String.valueOf(productId));

            for (int i = 0; i < keys.length; i++) {
                insertNewAttribute(moduleProductId, keys[i], values[i]);
            }
        } catch (SQLException e) {
            System.err.println("MariaDBDriver.addProductInfo ERROR:"
                                       + " Exception thrown (" + e.getMessage() + ").");
            return false;
        }

        return true;
    }

    @Override
    public long getMetadataLong(String key) {
        // TODO: Finish this
        return 0;
    }

    @Override
    public boolean setMetadata(String key, long value) {
        // TODO: Finish this
        return false;
    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            System.err.println("MariaDBDriver.close ERROR:"
                                       + " Exception thrown (" + e.getMessage() + ").");
        }
    }

    private String buildInsertTemplate(String tableName, long numFields, boolean setIgnore) {
        StringBuilder stringBuilder = new StringBuilder("INSERT ");

        if (setIgnore) {
            stringBuilder.append("IGNORE ");
        }

        stringBuilder.append("INTO ");
        stringBuilder.append(tableName);
        stringBuilder.append(" VALUES(");

        for (int i = 0; i < numFields; i++) {
            stringBuilder.append("?");
            if (i != numFields - 1) {
                stringBuilder.append(",");
            }
        }

        stringBuilder.append(")");
        return stringBuilder.toString();
    }

    private List<Long> insertIntoEntity(String name) throws SQLException {
        PreparedStatement statement;

        statement = connection.prepareStatement(buildInsertTemplate(ENTITY_TABLE, 2, false),
                                                Statement.RETURN_GENERATED_KEYS);
        statement.setNull(1, Types.INTEGER);
        statement.setString(2, name);
        statement.executeUpdate();


        ResultSet generatedKeys = statement.getGeneratedKeys();
        List<Long> keys = new ArrayList<Long>();

        while (generatedKeys.next()) {
            keys.add(generatedKeys.getLong(1));
        }

        generatedKeys.close();
        statement.close();

        return keys;
    }

    private List<Long> insertIntoPropertyType(String name, boolean isTrait) throws SQLException {
        PreparedStatement statement;

        statement = connection.prepareStatement(buildInsertTemplate(PROPERTY_TYPE_TABLE, 3, true),
                                                Statement.RETURN_GENERATED_KEYS);
        statement.setNull(1, Types.INTEGER);
        statement.setString(2, name);
        statement.setBoolean(3, isTrait);
        statement.executeUpdate();

        ResultSet generatedKeys = statement.getGeneratedKeys();
        List<Long> keys = new ArrayList<Long>();

        while (generatedKeys.next()) {
            keys.add(generatedKeys.getLong(1));
        }

        generatedKeys.close();
        statement.close();

        return keys;
    }

    private List<Long> insertIntoProperty(long entityId, long propertyTypeId) throws SQLException {
        PreparedStatement statement;

        statement = connection.prepareStatement(buildInsertTemplate(PROPERTY_TABLE, 3, false),
                                                Statement.RETURN_GENERATED_KEYS);
        statement.setNull(1, Types.INTEGER);
        statement.setLong(2, entityId);
        statement.setLong(3, propertyTypeId);
        statement.executeUpdate();

        ResultSet generatedKeys = statement.getGeneratedKeys();
        List<Long> keys = new ArrayList<Long>();

        while (generatedKeys.next()) {
            keys.add(generatedKeys.getLong(1));
        }

        generatedKeys.close();
        statement.close();

        return keys;
    }

    private List<Long> insertIntoTrait(long propertyId, String value) throws SQLException {
        PreparedStatement statement;

        statement = connection.prepareStatement(buildInsertTemplate(TRAIT_TABLE, 2, false),
                                                Statement.RETURN_GENERATED_KEYS);
        statement.setLong(1, propertyId);
        statement.setString(2, value);
        statement.executeUpdate();

        ResultSet generatedKeys = statement.getGeneratedKeys();
        List<Long> keys = new ArrayList<Long>();

        while (generatedKeys.next()) {
            keys.add(generatedKeys.getLong(1));
        }

        generatedKeys.close();
        statement.close();

        return keys;
    }

    private void insertNewAttribute(String moduleProductId, String key,
                                    String value) throws SQLException {
        long entityId = insertIntoEntity(moduleProductId).get(0);
        long propertyTypeId = insertIntoPropertyType(key, true).get(0);
        long propertyId = insertIntoProperty(entityId, propertyTypeId).get(0);
        insertIntoTrait(propertyId, value);
    }
}

class ResultSetIterator implements Iterator<String> {

    private final ResultSet resultSet;

    public ResultSetIterator(ResultSet resultSet) {
        this.resultSet = resultSet;
    }

    @Override
    public boolean hasNext() {
        try {
            return !resultSet.isAfterLast();
        } catch (SQLException e) {
            System.err.println("ResultSetIterator.hasNext ERROR:"
                                       + " Exception thrown (" + e.getMessage() + ").");
            return false;
        }
    }

    @Override
    public String next() {
        try {
            if (resultSet.next()) {
                return resultSet.getString(0);
            } else {
                return null;
            }
        } catch (SQLException e) {
            System.err.println("ResultSetIterator.next ERROR:"
                                       + " Exception thrown (" + e.getMessage() + ").");
            return null;
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Cannot remove from ResultSet.");
    }
}

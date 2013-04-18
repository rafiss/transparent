package transparent.core.database;

import transparent.core.Module;
import transparent.core.ProductID;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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

        System.err.println("Connecting to database...");
        connection = DriverManager.getConnection(host, username, password);
        System.err.println("Successfully connected to database...");
    }

    @Override
    public boolean addProductIds(Module module, String... moduleProductIds) {
        try {
            connection.setAutoCommit(false);

            for (String moduleProductId : moduleProductIds) {
                if (checkModuleProductExistence(moduleProductId, module).next()) {
                    continue;
                }

                // Insert moduleProductId as Entity
                long generatedEntityKey = insertIntoEntity(moduleProductId).get(0);
                insertNewAttribute(generatedEntityKey, MODULE_ID, module.getIdString());
            }
            connection.commit();
        } catch (SQLException e) {
            System.err.println("MariaDBDriver.addProductIds ERROR:"
                                       + " Exception thrown (" + e.getMessage() + ").");
            return false;
        } finally {
            try {
                connection.setAutoCommit(true);
            } catch (SQLException e) {
                System.err.println("MariaDBDriver.addProductIds ERROR:"
                                           + " Exception thrown (" + e.getMessage() + ").");
            }
        }

        return true;
    }

    @Override
    public Iterator<ProductID> getProductIds(Module module) {
        PreparedStatement statement = null;
        try {
            String[] columns = new String[] { "PropertyName", "TraitValue" };
            statement = connection.prepareStatement(buildSelectTemplate(MODEL_NAME,
                                                                        "EntityID, EntityName",
                                                                        "EntityID",
                                                                        columns));
            statement.setString(1, MODULE_ID);
            statement.setString(2, module.getIdString());
            return new ResultSetIterator(statement.executeQuery());
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
    @SafeVarargs
    public final boolean addProductInfo(Module module,
                                        ProductID productId,
                                        Entry<String, String>... keyValues) {
        try {
            for (Entry<String, String> pair : keyValues) {
                insertNewAttribute(productId.getRowId(), pair.getKey(), pair.getValue());
            }
        } catch (SQLException e) {
            module.logError("MariaDBDriver", "addProductInfo",
                            "Exception thrown (" + e.getMessage() + ").");
            return false;
        }

        return true;
    }

    @Override
    public String getMetadata(String key) {
        PreparedStatement statement = null;

        try {
            String[] columns = new String[] { "meta_key" };
            statement = connection.prepareStatement(buildSelectTemplate(METADATA_TABLE,
                                                                        "meta_value", null,
                                                                        columns));
            statement.setString(1, key);

            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getString(1);
            } else {
                return null;
            }
        } catch (SQLException e) {
            System.err.println("MariaDBDriver.getMetadata ERROR:"
                                       + " Exception thrown (" + e.getMessage() + ").");
            return null;
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    System.err.println("MariaDBDriver.getMetadata ERROR:"
                                               + " Exception thrown (" + e.getMessage() + ").");
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
                String whereField = "meta_key='" + key + "'";
                String[] columns = new String[] { "meta_value" };
                statement = connection.prepareStatement(buildUpdateTemplate(METADATA_TABLE,
                                                                            whereField,
                                                                            columns));
                statement.setString(1, value);
                statement.executeUpdate();
            } else {
                statement = connection.prepareStatement(buildInsertTemplate(METADATA_TABLE, 2));
                statement.setString(1, key);
                statement.setString(2, value);
                statement.executeUpdate();
            }

            connection.commit();
            return true;
        } catch (SQLException e) {
            System.err.println("MariaDBDriver.getMetadata ERROR:"
                                       + " Exception thrown (" + e.getMessage() + ").");
            return false;
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                    connection.setAutoCommit(true);
                } catch (SQLException e) {
                    System.err.println("MariaDBDriver.getMetadata ERROR:"
                                               + " Exception thrown (" + e.getMessage() + ").");
                }
            }
        }
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

    private String buildInsertTemplate(String tableName, long numFields) {
        StringBuilder stringBuilder = new StringBuilder("INSERT ");

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

    private String buildSelectTemplate(String tableName, String selectField, String groupBy,
                                       String... whereFields) {
        StringBuilder stringBuilder = new StringBuilder("SELECT ");

        stringBuilder.append(selectField);
        stringBuilder.append(" FROM ");
        stringBuilder.append(tableName);
        stringBuilder.append(" WHERE ");

        for (int i = 0; i < whereFields.length; i++) {
            stringBuilder.append(whereFields[i]);
            stringBuilder.append("=?");
            if (i != whereFields.length - 1) {
                stringBuilder.append(" AND ");
            }
        }

        if (groupBy != null) {
            stringBuilder.append(" GROUP BY ");
            stringBuilder.append(groupBy);
        }
        return stringBuilder.toString();
    }

    private String buildUpdateTemplate(String tableName, String whereField, String... columns) {
        StringBuilder stringBuilder = new StringBuilder("UPDATE ");

        stringBuilder.append(tableName);
        stringBuilder.append(" SET ");

        for (int i = 0; i < columns.length; i++) {
            stringBuilder.append(columns[i]);
            stringBuilder.append("=?");
            if (i != columns.length - 1) {
                stringBuilder.append(" AND ");
            }
        }

        stringBuilder.append(" WHERE ");
        stringBuilder.append(whereField);
        return stringBuilder.toString();
    }

    private ResultSet checkModuleProductExistence(String moduleProductId, Module module) {
        PreparedStatement statement = null;

        try {
            String[] columns = new String[] { "PropertyName", "TraitValue", "EntityName" };
            statement = connection.prepareStatement(buildSelectTemplate(MODEL_NAME, "*", null,
                                                                        columns));
            statement.setString(1, MODULE_ID);
            statement.setString(2, module.getIdString());
            statement.setString(3, moduleProductId);
            return statement.executeQuery();
        } catch (SQLException e) {
            System.err.println("MariaDBDriver.getMetadata ERROR:"
                                       + " Exception thrown (" + e.getMessage() + ").");
            return null;
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    System.err.println("MariaDBDriver.checkModuleProductExistence ERROR:"
                                               + " Exception thrown (" + e.getMessage() + ").");
                }
            }
        }
    }

    private ResultSet checkPropertyTypeExistence(String name) {
        PreparedStatement statement = null;

        try {
            String[] columns = new String[] { "property_name" };
            statement = connection.prepareStatement(buildSelectTemplate(PROPERTY_TYPE_TABLE,
                                                                        "property_type_id", null,
                                                                        columns));
            statement.setString(1, name);
            return statement.executeQuery();
        } catch (SQLException e) {
            System.err.println("MariaDBDriver.getMetadata ERROR:"
                                       + " Exception thrown (" + e.getMessage() + ").");
            return null;
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    System.err.println("MariaDBDriver.checkPropertyTypeExistence ERROR:"
                                               + " Exception thrown (" + e.getMessage() + ").");
                }
            }
        }
    }

    private ResultSet checkPropertyExistence(long entityId, long propertyTypeId) {
        PreparedStatement statement = null;

        try {
            String[] columns = new String[] { "entity_id", "property_type_id" };
            statement = connection.prepareStatement(buildSelectTemplate(PROPERTY_TABLE,
                                                                        "property_id", null,
                                                                        columns));
            statement.setLong(1, entityId);
            statement.setLong(2, propertyTypeId);
            return statement.executeQuery();
        } catch (SQLException e) {
            System.err.println("MariaDBDriver.getMetadata ERROR:"
                                       + " Exception thrown (" + e.getMessage() + ").");
            return null;
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    System.err.println("MariaDBDriver.checkPropertyExistence ERROR:"
                                               + " Exception thrown (" + e.getMessage() + ").");
                }
            }
        }
    }

    private ResultSet checkTraitExistence(long propertyId) {
        PreparedStatement statement = null;

        try {
            String[] columns = new String[] { "property_id" };
            statement = connection.prepareStatement(buildSelectTemplate(TRAIT_TABLE,
                                                                        "property_id", null,
                                                                        columns));
            statement.setLong(1, propertyId);
            return statement.executeQuery();
        } catch (SQLException e) {
            System.err.println("MariaDBDriver.getMetadata ERROR:"
                                       + " Exception thrown (" + e.getMessage() + ").");
            return null;
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    System.err.println("MariaDBDriver.checkTraitExistence ERROR:"
                                               + " Exception thrown (" + e.getMessage() + ").");
                }
            }
        }
    }

    private List<Long> insertIntoEntity(String name) throws SQLException {
        PreparedStatement statement;

        statement = connection.prepareStatement(buildInsertTemplate(ENTITY_TABLE, 2),
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

    private List<Long> insertIntoPropertyType(String name) throws SQLException {
        PreparedStatement statement = null;

        try {
            connection.setAutoCommit(false);

            ResultSet results = checkPropertyTypeExistence(name);
            List<Long> keys = new ArrayList<Long>();

            if (results.next()) {
                keys.add(results.getLong(1));
            } else {
                statement = connection.prepareStatement(buildInsertTemplate(PROPERTY_TYPE_TABLE, 2),
                                                        Statement.RETURN_GENERATED_KEYS);
                statement.setNull(1, Types.INTEGER);
                statement.setString(2, name);
                statement.executeUpdate();

                results = statement.getGeneratedKeys();
            }

            while (results.next()) {
                keys.add(results.getLong(1));
            }

            results.close();

            if (statement != null) {
                statement.close();
            }

            return keys;
        } finally {
            connection.setAutoCommit(true);
        }
    }

    private List<Long> insertIntoProperty(long entityId, long propertyTypeId) throws SQLException {
        PreparedStatement statement = null;

        try {
            connection.setAutoCommit(false);

            ResultSet results = checkPropertyExistence(entityId, propertyTypeId);
            List<Long> keys = new ArrayList<Long>();

            if (results.next()) {
                keys.add(results.getLong(1));
            } else {
                statement = connection.prepareStatement(buildInsertTemplate(PROPERTY_TABLE, 3),
                                                        Statement.RETURN_GENERATED_KEYS);
                statement.setNull(1, Types.INTEGER);
                statement.setLong(2, entityId);
                statement.setLong(3, propertyTypeId);
                statement.executeUpdate();

                results = statement.getGeneratedKeys();
            }

            while (results.next()) {
                keys.add(results.getLong(1));
            }

            results.close();

            if (statement != null) {
                statement.close();
            }

            return keys;
        } finally {
            connection.setAutoCommit(true);
        }
    }

    private List<Long> insertIntoTrait(long propertyId, String value) throws SQLException {
        PreparedStatement statement;

        try {
            connection.setAutoCommit(false);

            ResultSet results = checkTraitExistence(propertyId);
            List<Long> keys = new ArrayList<Long>();

            if (results.next()) {
                String whereField = "property_id='" + propertyId + "'";
                String[] columns = new String[] { "value" };
                statement = connection.prepareStatement(buildUpdateTemplate(TRAIT_TABLE,
                                                                            whereField, columns),
                                                        Statement.RETURN_GENERATED_KEYS);
                statement.setString(1, value);
                statement.executeUpdate();

                keys.add(results.getLong(1));
            } else {
                statement = connection.prepareStatement(buildInsertTemplate(TRAIT_TABLE, 2),
                                                        Statement.RETURN_GENERATED_KEYS);
                statement.setLong(1, propertyId);
                statement.setString(2, value);
                statement.executeUpdate();

                results = statement.getGeneratedKeys();
            }

            while (results.next()) {
                keys.add(results.getLong(1));
            }

            results.close();
            statement.close();
            return keys;
        } finally {
            connection.setAutoCommit(true);
        }
    }

    private void insertNewAttribute(long entityId, String key,
                                    String value) throws SQLException {
        try {
            connection.setAutoCommit(false);
            long propertyTypeId = insertIntoPropertyType(key).get(0);
            long propertyId = insertIntoProperty(entityId, propertyTypeId).get(0);
            insertIntoTrait(propertyId, value);
            connection.commit();
        } finally {
            connection.setAutoCommit(true);
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException {
        // Test client

        Database database = new MariaDBDriver();

        // TODO: i changed the constructor for module
        Module testModule = null; //new Module(null, "source", "module", 1, null, false, false);

        database.addProductIds(testModule, "product1", "product2");
        database.addProductIds(testModule, "product1", "product2");
        database.addProductIds(testModule, "product1", "product2");
        database.addProductIds(testModule, "product1", "product2");


        Iterator<ProductID> productIDIterator = database.getProductIds(testModule);

        while (productIDIterator.hasNext()) {
            ProductID productID = productIDIterator.next();
            System.out.println(productID.getModuleProductId() + " " + productID.getRowId());

            SimpleEntry<String, String> entry = new SimpleEntry("foo", "bar");
            database.addProductInfo(testModule, productID, entry);
            entry = new SimpleEntry("baz", "grep");
            database.addProductInfo(testModule, productID, entry);
            entry = new SimpleEntry("something", "else");
            database.addProductInfo(testModule, productID, entry);
            entry = new SimpleEntry("something", "else2");
            database.addProductInfo(testModule, productID, entry);

        }

        database.setMetadata("key1", "value1");
        System.out.println(database.getMetadata("key1"));
        System.out.println(database.getMetadata("key2"));
        database.setMetadata("key1", "value2");
        System.out.println(database.getMetadata("key1"));
    }
}

class ResultSetIterator implements Iterator<ProductID> {

    private final ResultSet resultSet;

    public ResultSetIterator(ResultSet resultSet) {
        this.resultSet = resultSet;
    }

    @Override
    public boolean hasNext() {
        try {
            return !resultSet.isLast();
        } catch (SQLException e) {
            System.err.println("ResultSetIterator.hasNext ERROR:"
                                       + " Exception thrown (" + e.getMessage() + ").");
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

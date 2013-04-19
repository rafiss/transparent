package transparent.core.database;

import transparent.core.Core;
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

        Core.println("Connecting to database...");
        connection = DriverManager.getConnection(host, username, password);
        Core.println("Successfully connected to database...");
    }

    public Connection getConnection() {
        return connection;
    }

    @Override
    public boolean addProductIds(Module module, String... moduleProductIds) {
        try {
            connection.setAutoCommit(false);

            for (String moduleProductId : moduleProductIds) {
                if (checkModuleProductExistence(moduleProductId, module)) {
                    continue;
                }

                // Insert moduleProductId as Entity
                long generatedEntityKey = insertIntoEntity(moduleProductId);
                insertNewAttribute(module, generatedEntityKey, MODULE_ID, module.getIdString());
            }
            connection.commit();
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            module.logError("MariaDBDriver", "addProductIds", "", e.getMessage());
            return false;
        } finally {
            try {
                connection.setAutoCommit(true);
            } catch (SQLException e) {
                module.logError("MariaDBDriver", "addProductIds", "", e.getMessage());
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
            return new ResultSetIterator(module, statement.executeQuery());
        } catch (SQLException e) {
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
        try {
            connection.setAutoCommit(false);
            for (Entry<String, String> pair : keyValues) {
                insertNewAttribute(module, productId.getRowId(), pair.getKey(), pair.getValue());
            }
            connection.commit();
        } catch (SQLException e) {
            module.logError("MariaDBDriver", "addProductInfo", "", e.getMessage());
            return false;
        } finally {
            try {
                connection.setAutoCommit(true);
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
            Core.printError("MariaDBDriver", "getMetadata", "", e.getMessage());
            return null;
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    Core.printError("MariaDBDriver", "getMetadata", "", e.getMessage());
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
            Core.printError("MariaDBDriver", "setMetadata", "", e.getMessage());
            return false;
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                    connection.setAutoCommit(true);
                } catch (SQLException e) {
                    Core.printError("MariaDBDriver", "setMetadata", "", e.getMessage());
                }
            }
        }
    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            Core.printError("MariaDBDriver", "close", "", e.getMessage());
        }
    }

    private String buildReplaceTemplate(String tableName, long numFields) {
        StringBuilder stringBuilder = new StringBuilder("REPLACE ");

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

    private String buildSelectExistsTemplate(String tableName, String... whereFields) {
        StringBuilder stringBuilder = new StringBuilder("SELECT EXISTS(SELECT 1 FROM ");
        stringBuilder.append(tableName);
        stringBuilder.append(" WHERE ");

        for (int i = 0; i < whereFields.length; i++) {
            stringBuilder.append(whereFields[i]);
            stringBuilder.append("=?");
            if (i != whereFields.length - 1) {
                stringBuilder.append(" AND ");
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

    private boolean checkModuleProductExistence(String moduleProductId, Module module) {
        PreparedStatement statement = null;

        try {
            String[] columns = new String[] { "PropertyName", "TraitValue", "EntityName" };
            statement = connection.prepareStatement(buildSelectExistsTemplate(MODEL_NAME,
                                                                              columns));
            statement.setString(1, MODULE_ID);
            statement.setString(2, module.getIdString());
            statement.setString(3, moduleProductId);

            ResultSet resultSet = statement.executeQuery();

            if (resultSet.next()) {
                return resultSet.getInt(1) == 1;
            } else {
                throw new SQLException("Failed to generate result.");
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            module.logError("MariaDBDriver", "checkModuleProductExistence", "", e.getMessage());
            return false;
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    module.logError("MariaDBDriver", "checkModuleProductExistence", "", e.getMessage());
                }
            }
        }
    }

    private ResultSet checkPropertyTypeExistence(Module module, String name) {
        PreparedStatement statement = null;

        try {
            String[] columns = new String[] { "property_name" };
            statement = connection.prepareStatement(buildSelectTemplate(PROPERTY_TYPE_TABLE,
                                                                        "property_type_id", null,
                                                                        columns));
            statement.setString(1, name);
            return statement.executeQuery();
        } catch (SQLException e) {
            module.logError("MariaDBDriver", "checkPropertyTypeExistence", "", e.getMessage());
            return null;
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    module.logError("MariaDBDriver", "checkPropertyTypeExistence", "", e.getMessage());
                }
            }
        }
    }

    private ResultSet checkPropertyExistence(
            Module module, long entityId, long propertyTypeId) {
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
            module.logError("MariaDBDriver", "checkPropertyExistence", "", e.getMessage());
            return null;
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    module.logError("MariaDBDriver", "checkPropertyExistence", "", e.getMessage());
                }
            }
        }
    }

    private long insertIntoEntity(String name) throws SQLException {
        PreparedStatement statement;
        long rowId;

        statement = connection.prepareStatement(buildInsertTemplate(ENTITY_TABLE, 2),
                                                Statement.RETURN_GENERATED_KEYS);
        statement.setNull(1, Types.INTEGER);
        statement.setString(2, name);
        statement.executeUpdate();

        ResultSet generatedKeys = statement.getGeneratedKeys();

        if (generatedKeys.next()) {
            rowId = generatedKeys.getLong(1);
            generatedKeys.close();
            statement.close();
            return rowId;
        } else {
            throw new SQLException("Failed to generate key.");
        }
    }

    private long insertIntoPropertyType(
            Module module, String name) throws SQLException {
        PreparedStatement statement = null;
        long rowId;

        try {
            ResultSet results = checkPropertyTypeExistence(module, name);

            if (results != null && results.next()) {
                rowId = results.getLong(1);
                results.close();
                return rowId;
            } else {
                statement = connection.prepareStatement(buildInsertTemplate(PROPERTY_TYPE_TABLE,
                                                                            2),
                                                        Statement.RETURN_GENERATED_KEYS);
                statement.setNull(1, Types.INTEGER);
                statement.setString(2, name);
                statement.executeUpdate();

                results = statement.getGeneratedKeys();
                if (results.next()) {
                    rowId = results.getLong(1);
                    results.close();
                    return rowId;
                } else {
                    throw new SQLException("Failed to generate key.");
                }
            }
        } finally {
            if (statement != null) {
                statement.close();
            }
        }
    }

    private long insertIntoProperty(Module module,
                                    long entityId, long propertyTypeId) throws SQLException {
        PreparedStatement statement = null;
        long rowId;

        try {
            ResultSet results = checkPropertyExistence(module, entityId, propertyTypeId);

            if (results != null && results.next()) {
                rowId = results.getLong(1);
                results.close();
                return rowId;
            } else {
                statement = connection.prepareStatement(buildInsertTemplate(PROPERTY_TABLE, 3),
                                                        Statement.RETURN_GENERATED_KEYS);
                statement.setNull(1, Types.INTEGER);
                statement.setLong(2, entityId);
                statement.setLong(3, propertyTypeId);
                statement.executeUpdate();

                results = statement.getGeneratedKeys();
                if (results.next()) {
                    rowId = results.getLong(1);
                    results.close();
                    return rowId;
                } else {
                    throw new SQLException("Failed to generate key.");
                }
            }
        } finally {
            if (statement != null) {
                statement.close();
            }
        }
    }

    private void insertIntoTrait(long propertyId, String value) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(buildReplaceTemplate
                                                                          (TRAIT_TABLE, 2));
        statement.setLong(1, propertyId);
        statement.setString(2, value);
        statement.executeUpdate();
        statement.close();
    }

    private void insertNewAttribute(Module module, long entityId,
                                    String key, String value) throws SQLException {
        long propertyTypeId = insertIntoPropertyType(module, key);
        long propertyId = insertIntoProperty(module, entityId, propertyTypeId);
        insertIntoTrait(propertyId, value);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException {
        // Test client

        MariaDBDriver database = new MariaDBDriver();

        // TODO: i changed the constructor for module
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


        while (productIDIterator.hasNext()) {
            ProductID productID = productIDIterator.next();
            //Core.println(productID.getModuleProductId() + " " + productID.getRowId());

            AbstractMap.SimpleEntry<String, String> entry = new AbstractMap.SimpleEntry("foo", "bar");
            database.addProductInfo(testModule, productID, entry);
            entry = new AbstractMap.SimpleEntry("baz", "grep");
            database.addProductInfo(testModule, productID, entry);
            entry = new AbstractMap.SimpleEntry("something", "else");
            database.addProductInfo(testModule, productID, entry);
            entry = new AbstractMap.SimpleEntry("something", "else2");
            database.addProductInfo(testModule, productID, entry);

        }

        database.setMetadata("key1", "value1");
        Core.println(database.getMetadata("key1"));
        Core.println(database.getMetadata("key2"));
        database.setMetadata("key1", "value2");
        Core.println(database.getMetadata("key1"));
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
                return new ProductID(resultSet.getLong(1), resultSet.getString(2));
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

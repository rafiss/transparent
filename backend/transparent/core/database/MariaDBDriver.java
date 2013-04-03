package transparent.core.database;

import transparent.core.Module;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class MariaDBDriver implements Database {

    private static final String TABLE_NAME = "catalog";
    private static final String CFG_FILE = "transparent/core/database/transparent.cfg";

    private final Connection connection;

    public MariaDBDriver() throws SQLException, IOException, ClassNotFoundException {
        Properties properties = new Properties();
        properties.load(new FileInputStream(CFG_FILE));

        String host = properties.getProperty("host");
        String username = properties.getProperty("username");
        String password = properties.getProperty("password");
        String driver = properties.getProperty("driver");

        /* register JDBC driver class */
        Class.forName(driver);

        System.err.println("Connecting to database...");
        connection = DriverManager.getConnection(host, username, password);
        System.err.println("Successfully connected to database...");
    }

    @Override
    public void addProductId(Module module, String productId) throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute("INSERT INTO " + TABLE_NAME + " VALUES (" +
                            module.getSourceName() + "," +
                            module.getModuleName() + "," +
                            productId + ")");
        statement.close();
    }

    @Override
    public void closeConnection() throws SQLException {
        connection.close();
    }
}

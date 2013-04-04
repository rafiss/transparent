package transparent.core.database;

import transparent.core.Module;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
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

    public void addProductId(Module module, String productId) throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute("INSERT INTO " + TABLE_NAME + " VALUES (" +
                          "'" + module.getSourceName() + "'," +
                          "'" + module.getModuleName() + "'," +
                          "'" + productId + "')");
        statement.close();
    }

    public void closeConnection() throws SQLException {
        
    }

	@Override
	public boolean addProductIds(Module module, String[] productIds) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Iterator<String> getProductIds(Module module) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean addProductInfo(Module module, String productId,
			String[] keys, String[] values) {
		// TODO Auto-generated method stub
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
}

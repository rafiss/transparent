package transparent.core.database;

import transparent.core.Module;

import java.sql.SQLException;

public interface Database
{
	public void addProductId(Module module, String productId) throws SQLException;

    public void closeConnection() throws SQLException;
}

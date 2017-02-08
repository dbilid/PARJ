package madgik.exareme.master.dbmanager;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.dbcp2.BasicDataSource;

public class DBManager {
	
	private final Map<String, BasicDataSource> sources;

	public DBManager() {
		super();
		this.sources=new HashMap<String, BasicDataSource>();
	}
	
	public Connection getConnection(String path) throws SQLException{
		if (!path.endsWith("/")) {
			path += "/";
		}
		path+="rdf.db";
		if(!sources.containsKey(path)){
			sources.put(path, createDataSource(path));
		}
		Connection c=sources.get(path).getConnection();
		//c.setAutoCommit(false);
		//Statement st=c.createStatement()
		return c;
		
	}
	
	private BasicDataSource createDataSource(String filepath){
		BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName("org.sqlite.JDBC");
        ds.setUsername("");
        ds.setPassword("");
        ds.setUrl("jdbc:sqlite:"+filepath);
       
        ds.setMinIdle(2);
        ds.setMaxIdle(10);
        ds.setMaxOpenPreparedStatements(30);
        
        ds.addConnectionProperty("synchronous", "false");
        ds.addConnectionProperty("auto_vacuum", "NONE");
        ds.addConnectionProperty("page_size", "4096");
        ds.addConnectionProperty("cache_size", "1048576");
        ds.addConnectionProperty("locking_mode", "EXCLUSIVE");
        ds.addConnectionProperty("journal_mode", "OFF");
        ds.addConnectionProperty("enable_load_extension", "true");
        
        return ds;
	}
	

}

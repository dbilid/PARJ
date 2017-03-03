package madgik.exareme.master.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.dbcp2.BasicDataSource;

import madgik.exareme.master.queryProcessor.decomposer.DecomposerUtils;

public class DBManager {

	private final Map<String, BasicDataSource> sources;

	public DBManager() {
		super();
		this.sources = new HashMap<String, BasicDataSource>();
	}

	public Connection getConnection(String path) throws SQLException {
		if (!path.endsWith("/")) {
			path += "/";
		}
		path += "rdf.db";
		if (!sources.containsKey(path)) {
			sources.put(path, createDataSource(path));
		}
		Connection c = sources.get(path).getConnection();

		// c.setAutoCommit(false);
		// Statement st=c.createStatement()
		return c;

	}

	private BasicDataSource createDataSource(String filepath) {
		BasicDataSource ds = new BasicDataSource();
		ds.setDriverClassName("org.sqlite.JDBC");
		ds.setUsername("");
		ds.setPassword("");
		ds.setUrl("jdbc:sqlite:" + filepath);

		ds.setMinIdle(5);
		ds.setMaxIdle(15);
		ds.setMaxOpenPreparedStatements(50);

		ds.addConnectionProperty("synchronous", "false");
		ds.addConnectionProperty("auto_vacuum", "NONE");
		ds.addConnectionProperty("page_size", "4096");
		ds.addConnectionProperty("cache_size", "1048576");
		ds.addConnectionProperty("locking_mode", "EXCLUSIVE");
		ds.addConnectionProperty("journal_mode", "OFF");
		ds.addConnectionProperty("enable_load_extension", "true");

		Set<String> init = new HashSet<String>(2);
		init.add("select load_extension('" + DecomposerUtils.WRAPPER_VIRTUAL_TABLE + "')");
		init.add("select load_extension('" + DecomposerUtils.INVWRAPPER_VIRTUAL_TABLE + "')");
		ds.setConnectionInitSqls(init);

		return ds;
	}

}

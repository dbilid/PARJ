package madgik.exareme.master.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.dbcp2.BasicDataSource;

import madgik.exareme.master.queryProcessor.decomposer.ParjUtils;

public class DBManager {

	private final Map<String, BasicDataSource> sources;

	public DBManager() {
		super();
		this.sources = new HashMap<String, BasicDataSource>();
	}

	public Connection getConnection(String path, int partitions) throws SQLException {
		if (!path.endsWith("/")) {
			path += "/";
		}
		path += "rdf.db";
		if (!sources.containsKey(path)) {
			sources.put(path, createDataSource(path, partitions+2));
		}
		Connection c = sources.get(path).getConnection();

		// c.setAutoCommit(false);
		// Statement st=c.createStatement()
		return c;

	}

	private BasicDataSource createDataSource(String filepath, int maxOpen) {
		BasicDataSource ds = new BasicDataSource();
		ds.setDriverClassName("org.sqlite.JDBC");
		ds.setUsername("");
		ds.setPassword("");
		ds.setUrl("jdbc:sqlite::memory:");
		//ds.setUrl("jdbc:sqlite:" + filepath);
		ds.setMinIdle(maxOpen);
		ds.setMaxIdle(maxOpen+1);
		ds.setMaxOpenPreparedStatements(maxOpen+2);
		ds.setMaxTotal(maxOpen+1);
		

		ds.addConnectionProperty("synchronous", "OFF");
		ds.addConnectionProperty("auto_vacuum", "NONE");
		ds.addConnectionProperty("page_size", "4096");
		ds.addConnectionProperty("cache_size", "1048576");
		ds.addConnectionProperty("locking_mode", "EXCLUSIVE");
		ds.addConnectionProperty("count_changes" ,"OFF");
		ds.addConnectionProperty("journal_mode", "OFF");
		ds.addConnectionProperty("enable_load_extension", "true");
		ds.addConnectionProperty("shared_cache", "false");
		ds.addConnectionProperty("read_uncommited", "false");
		ds.addConnectionProperty("temp_store", "MEMORY");
		ds.addConnectionProperty("ignore_check_constraints", "true");
		Set<String> init = new HashSet<String>(2);
		if(!filepath.equals("memory/rdf.db")){
			init.add("attach database '"+filepath+"' as m");
		}
		init.add("select load_extension('" + ParjUtils.WRAPPER_VIRTUAL_TABLE + "')");
		ds.setConnectionInitSqls(init);

		return ds;
	}

}

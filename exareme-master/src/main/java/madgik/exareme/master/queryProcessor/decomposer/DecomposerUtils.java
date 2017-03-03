package madgik.exareme.master.queryProcessor.decomposer;

import com.google.gson.Gson;
import madgik.exareme.utils.properties.AdpProperties;
import madgik.exareme.utils.properties.GenericProperties;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Map;

public class DecomposerUtils {

	public static final boolean CENTRALIZED;
	public static final boolean MULTI;
	public static final boolean ADD_NOT_NULLS;
	public static final boolean PROJECT_REF_COLS;
	public static final boolean ADD_ALIASES;
	public static final String EXTERNAL_KEY;
	public static final boolean IMPORT_EXTERNAL;
	public static final int FETCH_SIZE_ORACLE;
	public static final int FETCH_SIZE_POSTGRES;
	public static final int FETCH_SIZE_MYSQL;
	public static final Boolean RANDOM_TABLENAME_GENERATION;
	public static final String DECOMPOSER_LOG_LEVEL;
	public static final String ANALYZER_LOG_LEVEL;

	private static final Logger log = Logger.getLogger(DecomposerUtils.class);
	public static final int MAX_NUMBER_OF_UNIONS;
	public static final int NO_OF_RECORDS;
	public static final boolean USE_POSTGRES_COPY;
	public static final boolean ADD_TO_REGISTRY;
	public static final boolean PUSH_DISTINCT;

	public static final boolean USE_GROUP_BY;
	public static final boolean USE_ORDER_BY;
	public static final boolean REMOVE_OUTPUTS;
	public static final boolean PUSH_PROCESSING;
	public static final boolean WRITE_ALIASES;
	public static final double DISK_SCAN;
	public static final int MOST_PROMINENT;
	public static final boolean USE_GREEDY;
	public static final long EXPAND_DAG_TIME;
	public static final long ONLY_LEFT_TIME;
	public static final double DISTRIBUTED_LIMIT;
	public static final boolean CHOOSE_MODE;
	public static final int MERGE_UNIONS;
	public static final boolean USE_ROWID;
	public static final boolean USE_CROSS_JOIN;
	public static final boolean REPARTITION;
	public static final boolean PUSH_UNIONS;
	public static final boolean KILL_PYTHON;
	public static final boolean USE_SIP;
	public static final String WRAPPER_VIRTUAL_TABLE;
	public static final String INVWRAPPER_VIRTUAL_TABLE;

	static {
		GenericProperties properties = AdpProperties.getDecomposerProperties();
		DECOMPOSER_LOG_LEVEL = properties.getString("decomposer.logLevel");
		Logger.getLogger("madgik.exareme.master.queryProcessor.decomposer")
				.setLevel(Level.toLevel(DECOMPOSER_LOG_LEVEL));
		ANALYZER_LOG_LEVEL = properties.getString("analyzer.logLevel");
		Logger.getLogger("madgik.exareme.master.queryProcessor.analyzer").setLevel(Level.toLevel(ANALYZER_LOG_LEVEL));
		Logger.getLogger("madgik.exareme.master.queryProcessor.estimator").setLevel(Level.toLevel(ANALYZER_LOG_LEVEL));

		CENTRALIZED = properties.getBoolean("centralized");
		MULTI = properties.getBoolean("multi");
		ADD_NOT_NULLS = properties.getBoolean("addNotNulls");
		PROJECT_REF_COLS = properties.getBoolean("projectRefCols");
		ADD_ALIASES = properties.getBoolean("addAliases");
		EXTERNAL_KEY = properties.getString("externalKey");
		IMPORT_EXTERNAL = properties.getBoolean("importExternal");
		FETCH_SIZE_ORACLE = properties.getInt("fetchSize.oracle");
		FETCH_SIZE_POSTGRES = properties.getInt("fetchSize.postgres");
		FETCH_SIZE_MYSQL = properties.getInt("fetchSize.mysql");
		RANDOM_TABLENAME_GENERATION = properties.getBoolean("random.tablename.generation");
		MAX_NUMBER_OF_UNIONS = properties.getInt("max.number.of.unions");
		NO_OF_RECORDS = properties.getInt("number.of.records");
		USE_POSTGRES_COPY = properties.getBoolean("use.postgres.copy");
		ADD_TO_REGISTRY = properties.getBoolean("add.to.registry");
		PUSH_DISTINCT = properties.getBoolean("push.distinct");

		USE_GROUP_BY = properties.getBoolean("use.group.by");
		USE_ORDER_BY = properties.getBoolean("use.order.by");

		REMOVE_OUTPUTS = properties.getBoolean("remove.outputs");
		PUSH_PROCESSING = properties.getBoolean("push.processing");
		WRITE_ALIASES = properties.getBoolean("write.aliases");
		DISK_SCAN = properties.getFloat("disk.scan");
		MOST_PROMINENT = properties.getInt("most.prominent");
		USE_GREEDY = properties.getBoolean("use.greedy");
		EXPAND_DAG_TIME = properties.getLong("expand.dag.time");
		ONLY_LEFT_TIME = properties.getLong("only.left.time");
		DISTRIBUTED_LIMIT = properties.getLong("distributed.limit");
		CHOOSE_MODE = properties.getBoolean("choose.mode");
		MERGE_UNIONS = properties.getInt("merge.unions");
		USE_ROWID = properties.getBoolean("use.rowid");
		USE_CROSS_JOIN = properties.getBoolean("use.cross.join");
		REPARTITION = properties.getBoolean("repartition");
		PUSH_UNIONS = properties.getBoolean("push.unions");
		KILL_PYTHON = properties.getBoolean("kill.python");
		USE_SIP = false;
		WRAPPER_VIRTUAL_TABLE = properties.getString("wrapper.virtual.table");
		INVWRAPPER_VIRTUAL_TABLE = properties.getString("invwrapper.virtual.table");

		log.trace("Decomposer Properties Loaded.");
	}

	public static void getValues(String content, Map<String, String> dict) throws UnsupportedEncodingException {
		if (!content.isEmpty()) {
			try {
				getValuesFromJDBC(content, dict);
			} catch (Exception e) {
				getValuesFromWeb(content, dict);
			}
		}
	}

	private static void getValuesFromJDBC(String content, Map<String, String> dict)
			throws UnsupportedEncodingException {
		Gson g = new Gson();
		Map<String, String> values = g.fromJson(content, Map.class);
		dict.putAll(values);
	}

	private static void getValuesFromWeb(String content, Map<String, String> dict) throws UnsupportedEncodingException {
		String[] parts = content.split("&");
		for (String p : parts) {
			int split = p.indexOf("=");
			String key = p.substring(0, split);
			String value = p.substring(split + 1, p.length());
			dict.put(key, normalize(value));
		}
	}

	private static String normalize(String in) throws UnsupportedEncodingException {
		return URLDecoder.decode(in, "UTF-8");
	}
}

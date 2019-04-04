package madgik.exareme.master.queryProcessor.decomposer;

import com.google.gson.Gson;
import madgik.exareme.utils.properties.AdpProperties;
import madgik.exareme.utils.properties.GenericProperties;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Map;

public class ParjUtils {


	public static final String DECOMPOSER_LOG_LEVEL;
	public static final String ANALYZER_LOG_LEVEL;

	private static final Logger log = Logger.getLogger(ParjUtils.class);
	

	public static final double DISK_SCAN;
	public static final boolean USE_ROWID;
	public static final boolean USE_CROSS_JOIN;
	public static final String WRAPPER_VIRTUAL_TABLE;
	public static final boolean USE_RESULT_AGGREGATOR;
	public static final int SPLIT_BUCKET_THRESHOLD;
	public static final int CARDINALITY_THREADS;
	public static final int SKIP_TYPE_LIMIT;

	static {
		GenericProperties properties = AdpProperties.getDecomposerProperties();
		DECOMPOSER_LOG_LEVEL = properties.getString("decomposer.logLevel");
		Logger.getLogger("madgik.exareme.master.queryProcessor.decomposer")
				.setLevel(Level.toLevel(DECOMPOSER_LOG_LEVEL));
		ANALYZER_LOG_LEVEL = properties.getString("analyzer.logLevel");
		Logger.getLogger("madgik.exareme.master.queryProcessor.analyzer").setLevel(Level.toLevel(ANALYZER_LOG_LEVEL));
		Logger.getLogger("madgik.exareme.master.queryProcessor.estimator").setLevel(Level.toLevel(ANALYZER_LOG_LEVEL));

		

		
		DISK_SCAN = properties.getFloat("disk.scan");
		USE_ROWID = properties.getBoolean("use.rowid");
		USE_CROSS_JOIN = properties.getBoolean("use.cross.join");		
		WRAPPER_VIRTUAL_TABLE = properties.getString("wrapper.virtual.table");
		USE_RESULT_AGGREGATOR = properties.getBoolean("use.result.aggregator");
		SPLIT_BUCKET_THRESHOLD = properties.getInt("split.bucket.threshold");
		CARDINALITY_THREADS = properties.getInt("cardinality.threads");
		SKIP_TYPE_LIMIT = properties.getInt("skip.type.limit");
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

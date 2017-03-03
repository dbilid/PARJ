package madgik.exareme.master.queryProcessor.analyzer.fanalyzer;

import madgik.exareme.master.queryProcessor.analyzer.builder.HistogramBuildMethod;
import madgik.exareme.master.queryProcessor.analyzer.dbstats.ExternalGatherer;
import madgik.exareme.master.queryProcessor.analyzer.dbstats.Gatherer;
import madgik.exareme.master.queryProcessor.analyzer.dbstats.StatBuilder;
import madgik.exareme.master.queryProcessor.analyzer.stat.Table;
import madgik.exareme.master.queryProcessor.decomposer.federation.DB;
import madgik.exareme.master.queryProcessor.decomposer.federation.DBInfo;
import madgik.exareme.master.queryProcessor.decomposer.federation.DataImporter;
import madgik.exareme.master.queryProcessor.decomposer.query.SQLQuery;
import madgik.exareme.master.queryProcessor.estimator.db.Schema;
import madgik.exareme.utils.properties.AdpProperties;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author jim
 */
public class ExternalAnalyzer {
	public static final String GATHER_JSON = "";
	public static final String TMP_SAMPLE_DIR = "";
	public static final String BUILD_JSON = "";

	private static final Logger log = Logger.getLogger(ExternalAnalyzer.class);

	// public static final Map<String, Integer> tableCount = new HashMap<String,
	// Integer>();
	private String dbPath;
	private String schema;
	private Connection conn;

	public ExternalAnalyzer(String db, Connection c, String sch) throws Exception {
		this.dbPath = db;
		this.schema = sch;
		this.conn = c;
		if (!this.dbPath.endsWith("/")) {
			dbPath = dbPath + "/";
		}
	}

	public Schema analyzeAttrs(String tableName, Set<String> attrs) throws Exception {
		// createSample();
		// countRows();

		Map<String, Table> sch = gatherStats(tableName, attrs);
		log.debug("Stats for table " + tableName + " gathered. Building histograms...");
		Schema res = buildStats(sch, tableName);
		log.debug("Histigrams built for table " + tableName);
		// deleteSamples();
		return res;
		// this.con.close();
	}

	private Map<String, Table> gatherStats(String tableName, Set<String> attrs) throws Exception {
		// for (String s : this.statCols.keySet()) {
		ExternalGatherer g = new ExternalGatherer(conn, tableName, schema, attrs);
		return g.gather(dbPath);
		// }
	}

	private Schema buildStats(Map<String, Table> schema, String tableName) throws Exception {
		String[] db = new String[1];
		db[0] = tableName;
		StatBuilder sb = new StatBuilder(db, HistogramBuildMethod.Primitive, schema);
		return sb.build();
	}

}

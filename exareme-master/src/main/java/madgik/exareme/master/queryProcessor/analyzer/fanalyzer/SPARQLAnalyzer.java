package madgik.exareme.master.queryProcessor.analyzer.fanalyzer;

import java.sql.Connection;
import java.util.Map;

import madgik.exareme.master.queryProcessor.analyzer.builder.HistogramBuildMethod;
import madgik.exareme.master.queryProcessor.analyzer.dbstats.StatBuilder;
import madgik.exareme.master.queryProcessor.analyzer.stat.Stat;
import madgik.exareme.master.queryProcessor.analyzer.stat.Table;
import madgik.exareme.master.queryProcessor.estimator.db.Schema;

public class SPARQLAnalyzer {

	private Connection con;

	public SPARQLAnalyzer(Connection con) {
		super();
		this.con = con;
	}

	public Schema analyze() throws Exception {
		Stat stat = new Stat(con);
		stat.setSch("main");
		Map<String, Table> schema = stat.extractSPARQLStats();
		String[] db = new String[1];
		db[0] = "main";
		StatBuilder sb = new StatBuilder(db, HistogramBuildMethod.Primitive, schema);
		return sb.build();
	}

}

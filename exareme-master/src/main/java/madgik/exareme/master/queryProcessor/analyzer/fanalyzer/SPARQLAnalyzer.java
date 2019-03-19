package madgik.exareme.master.queryProcessor.analyzer.fanalyzer;

import java.sql.Connection;

import madgik.exareme.master.queryProcessor.analyzer.stat.MemoryStat;
import madgik.exareme.master.queryProcessor.analyzer.stat.Stat;
import madgik.exareme.master.queryProcessor.estimator.db.Schema;

public class SPARQLAnalyzer {

	private Connection con;
	private int properties;

	public SPARQLAnalyzer(Connection con, int properties) {
		super();
		this.con = con;
		this.properties=properties;
	}

	public Schema analyze() throws Exception {
		MemoryStat stat = new MemoryStat(con, properties);
		return stat.extractSPARQLStats();
		
	}

}

package madgik.exareme.master.queryProcessor.analyzer.fanalyzer;

import java.sql.Connection;

import madgik.exareme.master.queryProcessor.analyzer.stat.Stat;
import madgik.exareme.master.queryProcessor.estimator.db.Schema;

public class SPARQLAnalyzer {

	private Connection con;

	public SPARQLAnalyzer(Connection con) {
		super();
		this.con = con;
	}

	public Schema analyze() throws Exception {
		Stat stat = new Stat(con);
		return stat.extractSPARQLStats();
		
	}

}

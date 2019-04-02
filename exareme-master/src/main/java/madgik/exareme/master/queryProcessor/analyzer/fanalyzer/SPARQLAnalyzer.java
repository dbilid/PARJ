package madgik.exareme.master.queryProcessor.analyzer.fanalyzer;

import java.sql.Connection;

import madgik.exareme.master.db.DBManager;
import madgik.exareme.master.queryProcessor.analyzer.stat.MemoryStat;
import madgik.exareme.master.queryProcessor.analyzer.stat.Stat;
import madgik.exareme.master.queryProcessor.estimator.db.Schema;

public class SPARQLAnalyzer {

	private DBManager cons;
	private int properties;
	private String db;
	private int threads;
	

	public SPARQLAnalyzer(DBManager m, String database, int thrds, int properties) {
		super();
		this.cons = m;
		this.properties=properties;
		this.db=database;
		this.threads=thrds;
	}

	public Schema analyze() throws Exception {
		MemoryStat stat = new MemoryStat(cons, db, threads, properties);
		return stat.extractSPARQLStats();
		
	}

}

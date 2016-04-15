package madgik.exareme.master.queryProcessor.decomposer.util;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.sqlite.javax.SQLiteConnectionPoolDataSource;

import madgik.exareme.master.queryProcessor.analyzer.fanalyzer.OptiqueAnalyzer;
import madgik.exareme.master.queryProcessor.analyzer.stat.StatUtils;
import madgik.exareme.master.queryProcessor.decomposer.DecomposerUtils;
import madgik.exareme.master.queryProcessor.decomposer.federation.DB;
import madgik.exareme.master.queryProcessor.decomposer.federation.DataImporter;
import madgik.exareme.master.queryProcessor.decomposer.query.Column;
import madgik.exareme.master.queryProcessor.decomposer.query.SQLQuery;
import madgik.exareme.master.queryProcessor.decomposer.query.Table;
import madgik.exareme.master.queryProcessor.estimator.db.Schema;

public class PostgresImporter {

	public static void main(String[] args) throws Exception {
		boolean importtables=false;
		boolean analyze=false;
		boolean analyzeSQLITE=true;
		String path="/media/dimitris/T/exaremenpd100b/";
		DB dbinfo=new DB("ex");
		dbinfo.setSchema("public");
		dbinfo.setDriver("org.postgresql.Driver");
		dbinfo.setPass("gray769watt724!@#");
		dbinfo.setUser("postgres");
		dbinfo.setMadisString("postgres h:localhost port:5432 u:postgres p:gray769watt724!@# db:npd_vig_scale100");
		dbinfo.setURL("jdbc:postgresql://localhost/npd_vig_scale100");
		String url = "jdbc:postgresql://localhost/npd_vig_scale100";
		Properties props = new Properties();
		props.setProperty("user","postgres");
		props.setProperty("password","gray769watt724!@#");
		props.setProperty("ssl","true");
		Connection conn = DriverManager.getConnection(url, props);
		DatabaseMetaData md = conn.getMetaData();
		ResultSet rs = md.getTables(null, "public", "%", new String[] {"TABLE"});
		while (rs.next()) {
			String tablename=rs.getString(3);
			//if(!tablename.equalsIgnoreCase("wellbore_development_all")&&!tablename.equalsIgnoreCase("wellbore_exploration_all")&&!tablename.equalsIgnoreCase("wellbore_core")){
			//	continue;
			//}
			SQLQuery s=new SQLQuery();
			s.setFederated(true);
			s.setMadisFunctionString("postgres h:localhost u:postres");
			s.setTemporaryTableName(tablename);
			s.addInputTable(new Table(tablename, tablename));
			ResultSet rs2 = md.getColumns(null, null, rs.getString(3), null);
			Set<String> attrs = new HashSet<String>();

	            while (rs2.next()) {
	            	String columnname=rs2.getString(4);
	            //	if(!columnname.equalsIgnoreCase("dscNpdidDiscovery")){
	            //		continue;
	           // 	}
	            	attrs.add(columnname);
	                //Column c=new Column(tablename, columnname);
	                s.addOutput(tablename, columnname);
	            }
	            rs2.close();
	            
	            /*ResultSet rs3 = md.getIndexInfo(null, null, rs.getString(3), false, false);
	            while (rs3.next()) {
	            	System.out.println("Index for table: "+tablename+" column: "+rs3.getString(9));
	            }
	            rs3.close();*/
	            if(importtables){
	           ExecutorService es = Executors.newCachedThreadPool();
						DataImporter di = new DataImporter(s, path, dbinfo);
						di.setAddToRegisrty(true);
						es.execute(di);
				es.shutdown();
				es.awaitTermination(30, TimeUnit.MINUTES);
	            }
	            if(analyze){
	            	

					OptiqueAnalyzer fa = new OptiqueAnalyzer(path, dbinfo);
					fa.setUseDataImporter(true);
					System.out.println("analyzing: "+tablename);
					Schema sch = fa.analyzeAttrs(tablename, attrs);
					// change table name back to adding DB id
					
					StatUtils.addSchemaToFile(path + "histograms.json", sch);
	            }
	            if(analyzeSQLITE){
	            	Class.forName("org.sqlite.JDBC");
	   			 org.sqlite.SQLiteConfig config = new org.sqlite.SQLiteConfig();
	   			 config.setCacheSize(1200000);
	   			 config.setPageSize(4096);
	   			 SQLiteConnectionPoolDataSource dataSource = new SQLiteConnectionPoolDataSource();
	   			    dataSource.setUrl("jdbc:sqlite:"+path+tablename+".0.db");
	   			    dataSource.setConfig(config);
	   			Connection connection=dataSource.getConnection();//DriverManager.getConnection("jdbc:sqlite:test.db");
	   			Statement st=connection.createStatement();
	   			st.execute("analyze "+tablename);
	   			st.close();
	   			connection.close();
	            }
		}
		rs.close();
		conn.close();

	}

}

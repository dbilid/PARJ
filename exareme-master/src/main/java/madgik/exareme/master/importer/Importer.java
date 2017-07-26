package madgik.exareme.master.importer;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.QueryParser;
import org.eclipse.rdf4j.query.parser.QueryParserUtil;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.NTriplesParserSettings;

import madgik.exareme.master.db.DBManager;
import madgik.exareme.master.db.FinalUnionExecutor;
import madgik.exareme.master.db.ResultBuffer;
import madgik.exareme.master.db.SQLiteLocalExecutor;
import madgik.exareme.master.queryProcessor.analyzer.fanalyzer.SPARQLAnalyzer;
import madgik.exareme.master.queryProcessor.analyzer.stat.StatUtils;
import madgik.exareme.master.queryProcessor.decomposer.dag.Node;
import madgik.exareme.master.queryProcessor.decomposer.dag.NodeHashValues;
import madgik.exareme.master.queryProcessor.decomposer.federation.Memo;
import madgik.exareme.master.queryProcessor.decomposer.federation.SinglePlan;
import madgik.exareme.master.queryProcessor.decomposer.federation.SinlgePlanDFLGenerator;
import madgik.exareme.master.queryProcessor.decomposer.query.Column;
import madgik.exareme.master.queryProcessor.decomposer.query.NonUnaryWhereCondition;
import madgik.exareme.master.queryProcessor.decomposer.query.Output;
import madgik.exareme.master.queryProcessor.decomposer.query.SQLQuery;
import madgik.exareme.master.queryProcessor.decomposer.query.Table;
import madgik.exareme.master.queryProcessor.estimator.NodeSelectivityEstimator;
import madgik.exareme.master.queryProcessor.estimator.db.Schema;
import madgik.exareme.master.queryProcessor.sparql.DagCreator;
import madgik.exareme.master.queryProcessor.sparql.DagExpander;
import madgik.exareme.master.queryProcessor.sparql.IdFetcher;

public class Importer {

	public static void main(String[] args) throws IOException, SQLException {	  
		
		boolean importData = args[0].equals("load");
		boolean analyzeOnly = args[0].equals("analyze");
		//boolean analyze = false;
		boolean run = true;
		//boolean createVirtualTables = false;
		boolean execute = args[0].equals("query");
		int partitions = Integer.parseInt(args[2]);
		String database=args[1];

		DBManager m = new DBManager();
		
		long start=System.currentTimeMillis();
		if(analyzeOnly){
			Connection mainCon=m.getConnection(database, 2);
			analyzeDB(mainCon, database);
			return;
		}
		if (importData) {
			
			Connection c=m.getConnection("memory", 2);
			
			
			//InputStream s = readFile(args[3]);
			String importToSqlite="create virtual table tmptable using importer(";
			importToSqlite+=database+"/rdf.db";
			importToSqlite+=", ";
			importToSqlite+=args[3];
			importToSqlite+=");";
			
			
			Statement st=c.createStatement();
			st.execute(importToSqlite);
			System.out.println("imported in:"+(System.currentTimeMillis()-start)+" ms");
			st.close();
			c.close();
			Connection mainCon=m.getConnection(database, 2);
			
			mainCon.createStatement().execute("VACUUM");
			System.out.println("vacuum in:"+(System.currentTimeMillis()-start)+" ms");
			analyzeDB(mainCon, database);
			System.out.println("analyze in:"+(System.currentTimeMillis()-start)+" ms");
			
			//c.close();
			System.out.println("commited"+(System.currentTimeMillis()-start)+" ms");
			//createVirtualTables(c, partitions, database);
			//System.out.println("vtables in:"+(System.currentTimeMillis()-start)+" ms");
			mainCon.close();
			return;
		}
		
		
		Connection single=m.getConnection(database, partitions);
		//single.setTransactionIsolation(single.TRANSACTION_READ_UNCOMMITTED);
		if(run){
		System.out.println("loading data in memory...");
			Statement st = single.createStatement();
			String load="create virtual table tmptable using memorywrapper(";
			load+=String.valueOf(partitions);
			load+=" -1, -1)";
			st.execute(load);
			st.close();
			
		
			System.out.println("data loaded"+(System.currentTimeMillis()-start)+" ms");
			createVirtualTables(single, partitions);
			warmUpDBManager(partitions, database, m);
			
		}
	

			try {
				String prefixes = "PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX ub:<http://swat.cse.lehigh.edu/onto/univ-bench.owl#> ";
				String q = "SELECT ?y ?a  WHERE { ?y rdf:type ?z . ?a rdf:type ?z }";
				String aaa = "SELECT ?x WHERE { ?x rdf:type ub:FullProfessor} ";
				String q2 = "SELECT ?x ?y ?z WHERE {  ?y rdf:type ub:FullProfessor . ?y ub:teacherOf ?z .  ?z rdf:type ub:Course . ?x ub:advisor ?y . ?x rdf:type ub:UndergraduateStudent . ?x ub:takesCourse ?z }";
				String q3 = "SELECT ?x ?y ?z WHERE {  ?y ub:teacherOf ?z .  ?z rdf:type ub:Course . ?x ub:advisor ?y .  ?x ub:takesCourse ?z }";

				String lubm1 = "SELECT ?x ?y ?z WHERE { ?z ub:subOrganizationOf ?y .  ?y rdf:type ub:University .  ?z rdf:type ub:Department .  ?x ub:memberOf ?z .  ?x rdf:type ub:GraduateStudent .  ?x ub:undergraduateDegreeFrom ?y . }";
				String lubm2 = "SELECT ?x WHERE { ?x rdf:type ub:Course . ?x ub:name ?y .}";
				String lubm7 = "SELECT ?x ?y ?z WHERE { ?y ub:teacherOf ?z .  ?y rdf:type ub:FullProfessor . ?z rdf:type ub:Course . ?x ub:advisor ?y . ?x rdf:type ub:UndergraduateStudent . ?x ub:takesCourse ?z }";
				String lubm3 = "SELECT ?x ?y ?z WHERE { ?x rdf:type ub:UndergraduateStudent. ?y rdf:type ub:University . ?z rdf:type ub:Department . ?x ub:memberOf ?z . ?z ub:subOrganizationOf ?y . ?x ub:undergraduateDegreeFrom ?y . }";
				String lubm5 = "SELECT ?x WHERE {?x ub:subOrganizationOf <http://www.Department0.University0.edu> . ?x rdf:type ub:ResearchGroup }";
				String lubm4="SELECT ?x WHERE { ?x ub:worksFor <http://www.Department0.University0.edu> .  ?x rdf:type ub:FullProfessor .?x ub:name ?y1 . ?x ub:emailAddress ?y2 . ?x ub:telephone ?y3.}";
				String lubm6="SELECT ?x ?y WHERE { ?y ub:subOrganizationOf <http://www.University0.edu>.  ?y rdf:type ub:Department .  ?x ub:worksFor ?y . ?x rdf:type ub:FullProfessor . }";
				single.setAutoCommit(false);
				//single.setReadOnly(true);
				NodeSelectivityEstimator nse = new NodeSelectivityEstimator(
						database + "histograms.json");
				NodeHashValues hashes = new NodeHashValues();
				hashes.setSelectivityEstimator(nse);
				IdFetcher fetcher = new IdFetcher(single);
				fetcher.loadProperties();
				
				QueryParser qp = QueryParserUtil.createParser(QueryLanguage.SPARQL);
				warmUpJVM(prefixes+q, partitions, hashes, fetcher);
			
				while(true){
					try{
				hashes.clear();
				Scanner reader = new Scanner(System.in);  // Reading from System.in
				System.out.println("Enter query: ");
				String query= reader.nextLine();
				start=System.currentTimeMillis();
				ParsedQuery pq2 = qp.parseQuery(query, null);
				System.out.println("query parsed"+(System.currentTimeMillis() - start));
				DagCreator creator = new DagCreator(pq2, partitions, hashes, fetcher);

				Node root = creator.getRootNode();
				System.out.println("root created"+(System.currentTimeMillis() - start));
				// System.out.println(System.currentTimeMillis()-start);
				// System.out.println(root.count(0));

				// System.out.println(root.dotPrint(new HashSet<Node>()));
				DagExpander expander = new DagExpander(root, hashes);
				expander.expand();
				System.out.println("dag expanded"+(System.currentTimeMillis() - start));
				// System.out.println(root.dotPrint(new HashSet<Node>()));
				Memo memo = new Memo();

				expander.getBestPlanCentralized(root, Double.MAX_VALUE, memo);
				System.out.println("plan found"+(System.currentTimeMillis() - start));
				SinlgePlanDFLGenerator dsql = new SinlgePlanDFLGenerator(root, memo);
				// dsql.setN2a(n2a);
				SQLQuery result = dsql.generate().get(0);
				System.out.println("dsql generated"+(System.currentTimeMillis() - start));
				// TODO add virtual table when needed
				//for (int i = 1; i < result.getInputTables().size(); i++) {
				//	Table t = result.getInputTables().get(i);
				//	t.setName("wrapper" + t.getName());
				//}

				// add dictionary lookups
				int out = 1;
				Map<Column, Column> toChange = new HashMap<Column, Column>();
				for (Column outCol : result.getAllOutputColumns()) {
					String alias = "d" + out;
					out++;
					result.addInputTable(new Table("dictionary", alias));
					NonUnaryWhereCondition dictJoin = new NonUnaryWhereCondition(outCol.clone(),
							new Column(alias, "id"), "=");
					result.addBinaryWhereCondition(dictJoin);
					toChange.put(outCol.clone(), new Column(alias, "uri"));
				}
				for (Output o : result.getOutputs()) {
					for (Column c : toChange.keySet()) {
						o.getObject().changeColumn(c, toChange.get(c));
					}
				}

				System.out.println(System.currentTimeMillis() - start);
				result.invertColumns();
				result.computeTableToSplit(partitions);
				// System.out.println( result.toDistSQL() );

				if (run) {
					start=System.currentTimeMillis();
					ExecutorService es = Executors.newFixedThreadPool(partitions+1);
					//ExecutorService es = Executors.newFixedThreadPool(2);

					// Connection ccc=getConnection("");
					List<SQLiteLocalExecutor> executors = new ArrayList<SQLiteLocalExecutor>();
					ResultBuffer globalBuffer = new ResultBuffer();
					Set<Integer> finishedQueries = new HashSet<Integer>();
					Connection[] cons=new Connection[partitions];
					for (int i = 0; i < partitions; i++) {
						// String sql=result.getSqlForPartition(i);
						cons[i]=m.getConnection(database, partitions);
						//createVirtualTables(cons[i], partitions);
						SQLiteLocalExecutor ex = new SQLiteLocalExecutor(result,
								cons[i], true, finishedQueries, i);
						
						ex.setGlobalBuffer(globalBuffer);
						executors.add(ex);

					}
					
					FinalUnionExecutor ex = new FinalUnionExecutor(globalBuffer, null, partitions);
					es.execute(ex);
					//System.out.println(System.currentTimeMillis() - start);
					for (SQLiteLocalExecutor exec : executors) {
						es.execute(exec);
					}
					es.shutdown();
					try {
						boolean finished = es.awaitTermination(300, TimeUnit.MINUTES);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					System.out.println(System.currentTimeMillis() - start);
					for(int i=0; i<partitions; i++){
						cons[i].close();
					}
				}

				// System.out.println(root.count(0));
				
				System.out.println("OK");
					} catch (Exception e) {
						// TODO Auto-generated catch block
						
						e.printStackTrace();
						continue;
					}
				}
			
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
			



	}

	private static void createVirtualTables(Connection c, int partitions) throws SQLException {
		Statement st = c.createStatement();
		ResultSet rs = st.executeQuery("select id from properties");
		Statement st2 = c.createStatement();
		while (rs.next()) {
			int propNo = rs.getInt(1);
			// for(int i=0;i<partitions;i++){
			// System.out.println("create virtual table
			// wrapperprop"+propNo+" using wrapper("+partitions+",
			// prop"+propNo+")");
			// System.out.println("create virtual table
			// wrapperinvprop"+propNo+" using wrapper("+partitions+",
			// invprop"+propNo+")");
			st2.executeUpdate("create virtual table if not exists memorywrapperprop" + propNo + " using memorywrapper(" + partitions
					+ ", " + propNo + ", 0)");
			st2.executeUpdate("create virtual table if not exists memorywrapperinvprop" + propNo + " using memorywrapper(" + partitions
					+ ", " + propNo + ", 1)");
			
			// st.execute("create virtual table
			// wrapperinvprop"+propNo+"_"+i+" using
			// wrapper(invprop"+propNo+"_"+i+", "+partitions+")");
			// }
		}
		st2.close();
		rs.close();
		st.close();
		//System.out.println("VTs created");
	}

	private static void analyzeDB(Connection c, String db) {
		SPARQLAnalyzer a = new SPARQLAnalyzer(c);
		try {

			Schema stats = a.analyze();
			StatUtils.addSchemaToFile(db + "histograms.json", stats);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	private static void warmUpDBManager(int partitions, String database, DBManager m) throws SQLException {
		System.out.println("warming up DB manager...");
		long start=System.currentTimeMillis();
		List<Connection> cons=new ArrayList<Connection>(partitions+2);
		for(int i=0;i<partitions+2;i++){
			Connection next=m.getConnection(database, partitions);
			createVirtualTables(next, partitions);
			cons.add(next);
		}
		for(int i=0;i<cons.size();i++){
			cons.get(i).close();
		}
		System.out.println("finished warming up DBManager in "+(System.currentTimeMillis() - start+ " ms"));
		
	}

	private static void warmUpJVM(String q, int partitions, NodeHashValues hashes, IdFetcher fetcher ) throws SQLException {
		//parse and optimize a simple query in order to for JVM to load relevant classes
		System.out.println("warming up JVM...");
		long start=System.currentTimeMillis();
		QueryParser pq = QueryParserUtil.createParser(QueryLanguage.SPARQL);
		
		DagCreator creator = new DagCreator(pq.parseQuery(q, null), partitions, hashes, fetcher);

		Node root = creator.getRootNode();
		//System.out.println("root created"+(System.currentTimeMillis() - start));
		// System.out.println(System.currentTimeMillis()-start);
		// System.out.println(root.count(0));

		// System.out.println(root.dotPrint(new HashSet<Node>()));
		DagExpander expander = new DagExpander(root, hashes);
		expander.expand();
		//System.out.println("dag expanded"+(System.currentTimeMillis() - start));
		// System.out.println(root.dotPrint(new HashSet<Node>()));
		Memo memo = new Memo();

		expander.getBestPlanCentralized(root, Double.MAX_VALUE, memo);
		//System.out.println("plan found"+(System.currentTimeMillis() - start));
		SinlgePlanDFLGenerator dsql = new SinlgePlanDFLGenerator(root, memo);
		// dsql.setN2a(n2a);
		SQLQuery result = dsql.generate().get(0);
		System.out.println("finished warming up JVM in "+(System.currentTimeMillis() - start+ " ms"));
		
	}

	public static InputStream readFile(String f) throws IOException {
		File initialFile = new File(f);
		return FileUtils.openInputStream(initialFile);
	}

}

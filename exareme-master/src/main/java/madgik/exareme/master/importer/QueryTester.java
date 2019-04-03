package madgik.exareme.master.importer;

import java.util.LinkedList;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.Future;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.QueryParser;
import org.eclipse.rdf4j.query.parser.QueryParserUtil;
import madgik.exareme.master.db.DBManager;
import madgik.exareme.master.db.FinalUnionExecutor;
import madgik.exareme.master.db.ResultBuffer;
import madgik.exareme.master.db.SQLiteLocalExecutor;
import madgik.exareme.master.queryProcessor.analyzer.fanalyzer.SPARQLAnalyzer;
import madgik.exareme.master.queryProcessor.analyzer.stat.StatUtils;
import madgik.exareme.master.queryProcessor.decomposer.DecomposerUtils;
import madgik.exareme.master.queryProcessor.decomposer.dag.NodeHashValues;
import madgik.exareme.master.queryProcessor.decomposer.query.Column;
import madgik.exareme.master.queryProcessor.decomposer.query.NonUnaryWhereCondition;
import madgik.exareme.master.queryProcessor.decomposer.query.Output;
import madgik.exareme.master.queryProcessor.decomposer.query.SQLColumn;
import madgik.exareme.master.queryProcessor.decomposer.query.SQLQuery;
import madgik.exareme.master.queryProcessor.decomposer.query.Table;
import madgik.exareme.master.queryProcessor.estimator.NodeSelectivityEstimator;
import madgik.exareme.master.queryProcessor.estimator.db.Schema;
import madgik.exareme.master.queryProcessor.sparql.DagCreator;
import madgik.exareme.master.queryProcessor.sparql.IdFetcher;

public class QueryTester {

	public static void main(String[] args) throws IOException, SQLException {

		// boolean createVirtualTables = false;
		if (args.length == 0) {
			System.out.println("Please give database directory as parameter");
			return;
		}
		String database = args[0];
		boolean loadDictionary;
		boolean lookups;
		boolean printResults;
		int threads = 1;
		boolean executeFromFile;

		loadDictionary = readYesNo("Load Dictionary in memory?");

		lookups = readYesNo("Use dictionary lookups for results (includes result tuple construction)?");
		printResults = readYesNo("Print results?");
		executeFromFile = readYesNo("Execute queries from File? (If \"n\" queries would be read from user input) ");
		threads = readThreads("give number of threads to be used: ");

		printSettings(loadDictionary, lookups, printResults, executeFromFile, threads);

		DBManager m = new DBManager();

		long start = System.currentTimeMillis();
		int statThreads=DecomposerUtils.CARDINALITY_THREADS;
                int warmUpThreads=threads;
                if(statThreads>warmUpThreads) {
                        warmUpThreads=statThreads;
                }
		Connection single = m.getConnection(database, warmUpThreads);
		// single.setTransactionIsolation(single.TRANSACTION_READ_UNCOMMITTED);

		System.out.println("Loading data in memory. Please wait...");
		Statement st = single.createStatement();
		String load = "create virtual table tmptable using memorywrapper(";
		load += String.valueOf(threads);
		if(loadDictionary) {
			load += " -2, -2)";
		}
		else {
			load += " -1, -1)";
		}
		
		st.execute(load);
		st.close();

		System.out.println("data loaded" + (System.currentTimeMillis() - start) + " ms");
		createVirtualTables(single, threads, loadDictionary);
		//int statThreads=DecomposerUtils.CARDINALITY_THREADS;
		//int warmUpThreads=threads;
		//if(statThreads>warmUpThreads) {
		//	warmUpThreads=statThreads;
		//}
		warmUpDBManager(threads, warmUpThreads, database, m, loadDictionary);

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
			String lubm4 = "SELECT ?x WHERE { ?x ub:worksFor <http://www.Department0.University0.edu> .  ?x rdf:type ub:FullProfessor .?x ub:name ?y1 . ?x ub:emailAddress ?y2 . ?x ub:telephone ?y3.}";
			String lubm6 = "SELECT ?x ?y WHERE { ?y ub:subOrganizationOf <http://www.University0.edu>.  ?y rdf:type ub:Department .  ?x ub:worksFor ?y . ?x rdf:type ub:FullProfessor . }";
			single.setAutoCommit(false);
			// single.setReadOnly(true);
			IdFetcher fetcher = new IdFetcher(single);
			fetcher.loadProperties();
			NodeSelectivityEstimator nse = null;
			try {
				nse = new NodeSelectivityEstimator(database + "histograms.json");
			} catch( FileNotFoundException fnt) {
				System.out.println("Database statistics are missing. Analyzing database (this may take some time...)");
				SPARQLAnalyzer a = new SPARQLAnalyzer(m, database, threads, fetcher.getPropertyCount());
				try {

					Schema stats = a.analyze();
					StatUtils.addSchemaToFile(database + "histograms.json", stats);
					nse = new NodeSelectivityEstimator(database + "histograms.json");
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			NodeHashValues hashes = new NodeHashValues();
			hashes.setSelectivityEstimator(nse);
			//Connection mainCon = m.getConnection(database, 2);

			//mainCon.close();
			
			try {
				long typeProperty = fetcher.getIdForProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
				nse.setRdfTypeTable((int) typeProperty);
			} catch (java.sql.SQLException ex) {
				System.out.println("no rdf:type property in data");
			}
			QueryParser qp = QueryParserUtil.createParser(QueryLanguage.SPARQL);
			warmUpJVM(prefixes + q, threads, hashes, fetcher);

			if (executeFromFile) {
				Scanner reader = new Scanner(System.in); // Reading from System.in
				System.out.println(
						"Give Filepath to File with Queries \n (Each query must be in a single line. Lines with less than 30 characters will be ignored)  : ");
				List<String> queries = readFile(reader.nextLine());

				// List<String> queries = readFile(args[3]);
				List<Long> times = new ArrayList<Long>(queries.size());
				List<Long> noOfResults = new ArrayList<Long>(queries.size());
				int qNo = -1;
				ExecutorService es = Executors.newFixedThreadPool(threads + 1);
				for (String query : queries) {
					qNo++;
					System.out.println(query);
					long queryTimes = 0L;
					for (int rep = 0; rep < 11; rep++) {
						// while(true){
						try {
							hashes.clear();

							start = System.currentTimeMillis();
							ParsedQuery pq2 = qp.parseQuery(query, null);
							System.out.println("query parsed at" + (System.currentTimeMillis() - start));
							DagCreator creator = new DagCreator(pq2, threads, hashes, fetcher);

							SQLQuery result = creator.getRootNode();
							System.out.println("query optimized at:" + (System.currentTimeMillis() - start));
							// System.out.println(System.currentTimeMillis()-start);

							//System.out.println(System.currentTimeMillis() - start);
							result.invertColumns();
							result.computeTableToSplit(threads);
							List<String> exatraCreates = result.computeExtraCreates(threads);

							if (lookups) {
								int out = 1;
								Map<Column, SQLColumn> toChange = new HashMap<Column, SQLColumn>();

								for (Column outCol : result.getAllOutputColumns()) {
									Table dict=null;
									if(loadDictionary) {
										dict = new Table(-2, -2);
									}
									else {
										dict = new Table(-1, -1);
									}
									dict.setDictionary(out);
									result.addInputTable(dict);
									NonUnaryWhereCondition dictJoin = new NonUnaryWhereCondition(outCol.clone(),
											new SQLColumn("d" + out, "id"), "=");
									result.addBinaryWhereCondition(dictJoin);
									toChange.put(outCol.clone(), new SQLColumn("d" + out, "uri"));
									out++;
								}

								for (Output o : result.getOutputs()) {
									for (Column c : toChange.keySet()) {
										if (o.getObject() instanceof Column) {
											Column c2 = (Column) o.getObject();
											if (c2.equals(c)) {
												o.setObject(toChange.get(c));
											}
										} else {
											// System.err.println("projection not column");
										}
										// o.getObject().changeColumn(c, toChange.get(c));
									}
								}
							}

							// start=System.currentTimeMillis();
							// ExecutorService es = Executors.newFixedThreadPool(partitions+1);
							// ExecutorService es = Executors.newFixedThreadPool(2);
							Collection<Future<?>> futures = new LinkedList<Future<?>>();
							// Connection ccc=getConnection("");
							// List<SQLiteLocalExecutor> executors = new ArrayList<SQLiteLocalExecutor>();
							ResultBuffer globalBuffer = new ResultBuffer();
							Set<Integer> finishedQueries = new HashSet<Integer>();
							Connection[] cons = new Connection[threads];
							for (int i = 0; i < threads; i++) {
								// String sql=result.getSqlForPartition(i);
								cons[i] = m.getConnection(database, warmUpThreads);

								// createVirtualTables(cons[i], partitions);
								SQLiteLocalExecutor ex = new SQLiteLocalExecutor(result, cons[i],
										DecomposerUtils.USE_RESULT_AGGREGATOR, finishedQueries, i, printResults,
										lookups, exatraCreates);

								ex.setGlobalBuffer(globalBuffer);
								// executors.add(ex);
								futures.add(es.submit(ex));
							}

							if (DecomposerUtils.USE_RESULT_AGGREGATOR) {
								FinalUnionExecutor ex = new FinalUnionExecutor(globalBuffer, null, threads, printResults);
								futures.add(es.submit(ex));
								// es.execute(ex);
							}
							// System.out.println(System.currentTimeMillis() - start);
							/*
							 * for (SQLiteLocalExecutor exec : executors) { futures.add(es.submit(ex));
							 * //es.execute(exec); }
							 */
							// es.shutdown();
							try {
								for (Future<?> future : futures) {
									future.get();
								}
								// boolean finished = es.awaitTermination(300, TimeUnit.MINUTES);
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							if (rep > 0)
								queryTimes += System.currentTimeMillis() - start;
							System.out.println("total execution time:" +(System.currentTimeMillis() - start));
							for (int i = 0; i < threads; i++) {
								cons[i].close();
							}
							if (!DecomposerUtils.USE_RESULT_AGGREGATOR) {
								System.out.println("total results:" + globalBuffer.getFinished());
								if (rep == 0)
									noOfResults.add(globalBuffer.getFinished());
							}

							// System.out.println(root.count(0));

							System.out.println("OK");
						} catch (Exception e) {
							// TODO Auto-generated catch block

							e.printStackTrace();
							continue;
						}
						// }
						// queryTimes=queryTimes/10;
						// times.add(queryTimes);
					}
					queryTimes = queryTimes / 10;
					if (queryTimes == 0)
						queryTimes = 1;
					times.add(queryTimes);
				}

				System.out.println(times);
				double sum = times.get(0);
				double realSum = times.get(0);
				for (int i = 1; i < times.size(); i++) {
					sum *= times.get(i);
					realSum += times.get(i);
				}
				System.out.println("geo mean: " + Math.pow(sum, 1.0 / times.size()));
				System.out.println("avg.: " + realSum / times.size());
				System.out.println("no of results:" + noOfResults);
			} else {
				ExecutorService es = Executors.newFixedThreadPool(threads + 1);
				while (true) {
					try {
						hashes.clear();
						Scanner reader = new Scanner(System.in); // Reading from
																	// System.in
						System.out.println("Enter query (in a single line) and press enter: ");
						String query = reader.nextLine();
						start = System.currentTimeMillis();
						ParsedQuery pq2 = qp.parseQuery(query, null);
						// System.out.println("query
						// parsed"+(System.currentTimeMillis() - start));
						DagCreator creator = new DagCreator(pq2, threads, hashes, fetcher);

						SQLQuery result = creator.getRootNode();
						System.out.println("query optimized at" + (System.currentTimeMillis() - start));
						// System.out.println(System.currentTimeMillis()-start);

						//System.out.println(System.currentTimeMillis() - start);
						result.invertColumns();
						result.computeTableToSplit(threads);
						List<String> exatraCreates = result.computeExtraCreates(threads);

						if (lookups) {
							// add dictionary lookups
							int out = 1;
							Map<Column, SQLColumn> toChange = new HashMap<Column, SQLColumn>();

							for (Column outCol : result.getAllOutputColumns()) {
								Table dict=null;
								if(loadDictionary) {
									dict = new Table(-2, -2);
								}
								else {
									dict = new Table(-1, -1);
								}
								dict.setDictionary(out);
								result.addInputTable(dict);
								NonUnaryWhereCondition dictJoin = new NonUnaryWhereCondition(outCol.clone(),
										new SQLColumn("d" + out, "id"), "=");
								result.addBinaryWhereCondition(dictJoin);
								toChange.put(outCol.clone(), new SQLColumn("d" + out, "uri"));
								out++;
							}

							for (Output o : result.getOutputs()) {
								for (Column c : toChange.keySet()) {
									if (o.getObject() instanceof Column) {
										Column c2 = (Column) o.getObject();
										if (c2.equals(c)) {
											o.setObject(toChange.get(c));
										}
									} else {
										// System.err.println("projection not column");
									}
									// o.getObject().changeColumn(c, toChange.get(c));
								}
							}
						}

						// start=System.currentTimeMillis();
						// ExecutorService es =
						// Executors.newFixedThreadPool(partitions+1);
						// ExecutorService es = Executors.newFixedThreadPool(2);

						// Connection ccc=getConnection("");
						// List<SQLiteLocalExecutor> executors = new
						// ArrayList<SQLiteLocalExecutor>();
						ResultBuffer globalBuffer = new ResultBuffer();
						Set<Integer> finishedQueries = new HashSet<Integer>();
						Connection[] cons = new Connection[threads];
						Collection<Future<?>> futures = new LinkedList<Future<?>>();
						for (int i = 0; i < threads; i++) {
							// String sql=result.getSqlForPartition(i);
							cons[i] = m.getConnection(database, warmUpThreads);

							// createVirtualTables(cons[i], partitions);
							SQLiteLocalExecutor ex = new SQLiteLocalExecutor(result, cons[i],
									DecomposerUtils.USE_RESULT_AGGREGATOR, finishedQueries, i, printResults,
									lookups, exatraCreates);

							ex.setGlobalBuffer(globalBuffer);
							// executors.add(ex);
							futures.add(es.submit(ex));
						}

						if (DecomposerUtils.USE_RESULT_AGGREGATOR) {
							FinalUnionExecutor ex = new FinalUnionExecutor(globalBuffer, null, threads, printResults);
							// es.execute(ex);
							futures.add(es.submit(ex));
						}
						// System.out.println(System.currentTimeMillis() -
						// start);
						/*
						 * for (SQLiteLocalExecutor exec : executors) { es.execute(exec); }
						 * es.shutdown();
						 */
						try {
							for (Future<?> future : futures) {
								future.get();
							}
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						System.out.println("total execution time:" + (System.currentTimeMillis() - start));
						for (int i = 0; i < threads; i++) {
							cons[i].close();
						}
						if (!DecomposerUtils.USE_RESULT_AGGREGATOR) {
							System.out.println("total results:" + globalBuffer.getFinished());
						}

						// System.out.println(root.count(0));

						System.out.println("OK");
					} catch (Exception e) {
						// TODO Auto-generated catch block

						e.printStackTrace();
						continue;
					}
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static void printSettings(boolean loadDictionary, boolean silent, boolean printResults, boolean executeFromFile, int threads) {
		System.out.println("Current Settings:");
		System.out.println("\tLoad Dictionary in memory: " + loadDictionary);
		System.out.println("\tUse Dictionary Lookups: " + silent);
		System.out.println("\tPrint Results: " + printResults);
		System.out.println("\tExecute Queries From File: " + executeFromFile);
		System.out.println("\tNumber of Threads: " + threads);

	}

	private static void createVirtualTables(Connection c, int partitions, boolean loadDictionary) throws SQLException {
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
			st2.executeUpdate("create virtual table if not exists memorywrapperprop" + propNo + " using memorywrapper("
					+ partitions + ", " + propNo + ", 0)");
			st2.executeUpdate("create virtual table if not exists memorywrapperinvprop" + propNo
					+ " using memorywrapper(" + partitions + ", " + propNo + ", 1)");

			// st.execute("create virtual table
			// wrapperinvprop"+propNo+"_"+i+" using
			// wrapper(invprop"+propNo+"_"+i+", "+partitions+")");
			// }
		}
		if(loadDictionary) {
			for(int i=0;i<100;i++) {
				st2.executeUpdate("create virtual table if not exists d" + i + " using dictionary()");
			}
		}
		st2.close();
		rs.close();
		st.close();
		// System.out.println("VTs created");
	}

	private static void warmUpDBManager(int threads, int partitions, String database, DBManager m, boolean loadDictionary) throws SQLException {
		System.out.println("warming up DB manager...");
		long start = System.currentTimeMillis();
		List<Connection> cons = new ArrayList<Connection>(partitions + 2);
		for (int i = 0; i < partitions + 2; i++) {
			Connection next = m.getConnection(database, partitions);
			createVirtualTables(next, threads, loadDictionary);
			cons.add(next);

		}
		
		for (int i = 0; i < cons.size(); i++) {
			cons.get(i).close();
		}
		System.out.println("finished warming up DBManager in " + (System.currentTimeMillis() - start + " ms"));

	}

	private static void warmUpJVM(String q, int partitions, NodeHashValues hashes, IdFetcher fetcher)
			throws SQLException {
		// parse and optimize a simple query in order to for JVM to load relevant
		// classes
		System.out.println("warming up JVM...");
		long start = System.currentTimeMillis();
		QueryParser pq = QueryParserUtil.createParser(QueryLanguage.SPARQL);

		DagCreator creator = new DagCreator(pq.parseQuery(q, null), partitions, hashes, fetcher);

		SQLQuery query = creator.getRootNode();
		query.computeTableToSplit(partitions);
		query.getSqlForPartition(0);
		// System.out.println("root created"+(System.currentTimeMillis() - start));
		// System.out.println(System.currentTimeMillis()-start);
		// System.out.println(root.count(0));

		// System.out.println(root.dotPrint(new HashSet<Node>()));

		// dsql.setN2a(n2a);
		System.out.println("finished warming up JVM in " + (System.currentTimeMillis() - start + " ms"));

	}

	private static List<String> readFile(String file) throws IOException {
		List<String> result = new ArrayList<String>();
		BufferedReader reader = new BufferedReader(new FileReader(file));
		String line = null;
		// StringBuilder stringBuilder = new StringBuilder();
		// String ls = System.getProperty("line.separator");

		while ((line = reader.readLine()) != null) {
			if (line.length() < 30)
				continue;
			result.add(line);
		}
		reader.close();
		return result;
	}

	private static boolean readYesNo(String q) {
		System.out.println(q + "[y/n] :");
		Scanner scanner = new Scanner(System.in);
		String answer = scanner.nextLine();
		if (answer.equalsIgnoreCase("y")) {
			return true;
		} else {
			return false;
		}
	}

	private static int readThreads(String q) {
		System.out.println("give number of threads :");
		String input = null;
		int result = 1;
		try {
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
			input = bufferedReader.readLine();
			result = Integer.parseInt(input);
		} catch (NumberFormatException ex) {
			System.out.println("Not a number. Number of threads set to 1 !");
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}

}

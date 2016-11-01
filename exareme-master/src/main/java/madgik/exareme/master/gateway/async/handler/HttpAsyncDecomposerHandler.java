package madgik.exareme.master.gateway.async.handler;

import com.google.gson.Gson;

import madgik.exareme.master.client.AdpDBClient;
import madgik.exareme.master.client.AdpDBClientFactory;
import madgik.exareme.master.client.AdpDBClientProperties;
import madgik.exareme.master.client.AdpDBClientQueryStatus;
import madgik.exareme.master.connector.AdpDBConnector;
import madgik.exareme.master.connector.AdpDBConnectorFactory;
import madgik.exareme.master.connector.local.AdpDBQueryExecutorThread;
import madgik.exareme.master.connector.rmi.AdpDBNetReaderThread;
import madgik.exareme.master.engine.intermediateCache.Cache;
import madgik.exareme.utils.association.Pair;
import madgik.exareme.utils.embedded.db.DBUtils;
import madgik.exareme.utils.properties.AdpDBProperties;
import madgik.exareme.master.engine.AdpDBManager;
import madgik.exareme.master.engine.AdpDBManagerLocator;
import madgik.exareme.master.gateway.ExaremeGatewayUtils;
import madgik.exareme.master.queryProcessor.analyzer.fanalyzer.ExternalAnalyzer;
import madgik.exareme.master.queryProcessor.analyzer.fanalyzer.OptiqueAnalyzer;
import madgik.exareme.master.queryProcessor.analyzer.stat.StatUtils;
import madgik.exareme.master.queryProcessor.decomposer.DecomposerUtils;
import madgik.exareme.master.queryProcessor.decomposer.dag.NodeHashValues;
import madgik.exareme.master.queryProcessor.decomposer.federation.DB;
import madgik.exareme.master.queryProcessor.decomposer.federation.DBInfoReaderDB;
import madgik.exareme.master.queryProcessor.decomposer.federation.DBInfoWriterDB;
import madgik.exareme.master.queryProcessor.decomposer.federation.NamesToAliases;
import madgik.exareme.master.queryProcessor.decomposer.federation.QueryDecomposer;
import madgik.exareme.master.queryProcessor.decomposer.query.SQLQuery;
import madgik.exareme.master.queryProcessor.decomposer.query.SQLQueryParser;
import madgik.exareme.master.queryProcessor.decomposer.query.Table;
import madgik.exareme.master.queryProcessor.estimator.NodeSelectivityEstimator;
import madgik.exareme.master.queryProcessor.estimator.db.Schema;
import madgik.exareme.worker.art.registry.ArtRegistryLocator;

import org.apache.http.*;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.DefaultNHttpClientConnection;
import org.apache.http.impl.nio.DefaultNHttpServerConnection;
import org.apache.http.nio.NHttpConnection;
import org.apache.http.nio.protocol.*;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.mortbay.jetty.HttpStatus;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Exareme Decomposer Handler.
 *
 * @author alex
 * @author Dimitris
 * @since 0.1
 */
public class HttpAsyncDecomposerHandler implements HttpAsyncRequestHandler<HttpRequest> {

    private static final Logger log = Logger.getLogger(HttpAsyncDecomposerHandler.class);
    private static final AdpDBManager manager = AdpDBManagerLocator.getDBManager();
    private static final boolean useCache = AdpDBProperties.getAdpDBProps()
            .getString("db.cache").equals("true");
    private static NamesToAliases n2a = new NamesToAliases();
    private static boolean killPython=true;

	public HttpAsyncDecomposerHandler() {
	}

	@Override
    public HttpAsyncRequestConsumer<HttpRequest> processRequest(final HttpRequest request,
                                                                final HttpContext context)
            throws HttpException, IOException {
		return new BasicAsyncRequestConsumer();
	}

	@Override
	public void handle(final HttpRequest httpRequest, final HttpAsyncExchange httpExchange, final HttpContext context)
			throws HttpException, IOException {
		final HttpResponse httpResponse = httpExchange.getResponse();
		log.trace("Parsing request ...");
		
		String method = httpRequest.getRequestLine().getMethod().toUpperCase(Locale.ENGLISH);
		if (!"GET".equals(method) && !"POST".equals(method))
			throw new UnsupportedHttpVersionException(method + "not supported.");
		String content = "";
		if (httpRequest instanceof HttpEntityEnclosingRequest) {
			//log.debug("Stream ...");
			HttpEntity entity = ((HttpEntityEnclosingRequest) httpRequest).getEntity();
			content = EntityUtils.toString(entity);
		}

		final HashMap<String, String> inputContent = new HashMap<String, String>();
		ExaremeGatewayUtils.getValues(content, inputContent);

		final String dbname = inputContent.get(ExaremeGatewayUtils.REQUEST_DATABASE);
		final String query = inputContent.get(ExaremeGatewayUtils.REQUEST_QUERY);
		final int workers = ArtRegistryLocator.getArtRegistryProxy().getContainers().length;

		log.debug("--DB " + dbname);
		if(!query.startsWith("addFederatedEndpoint(")){
			log.debug("--Query " + query);
		}

		log.trace("Decomposing ...");
		new Thread() {
			@Override
			public void run() {
				DefaultNHttpServerConnection serverCon=(DefaultNHttpServerConnection)context.getAttribute("http.connection");
				try {
					if (query.startsWith("addFederatedEndpoint")) {

						log.debug("Adding endpoint to : " + dbname + "endpoint.db ...");
						DBInfoWriterDB.write(query, dbname);
						InputStreamEntity se = new InputStreamEntity(createOKResultStream(), -1,
								ContentType.TEXT_PLAIN);
						log.debug("Sending OK : " + se.toString());
						httpResponse.setEntity(se);
					} else if (query.startsWith("getTables")) {
						Class.forName("org.sqlite.JDBC");
						String path = dbname;
						if (!path.endsWith("/")) {
							path += "/";
						}

						Connection c = createDummyDB(path);

						// Connection c = cons.get(path);
						// Connection c = DriverManager
						// .getConnection("jdbc:sqlite:" + path+"registry.db");
						String[] params = query.split(" ");
						for (int i = 1; i < params.length; i++) {
							if (params[i].equalsIgnoreCase("null")) {
								params[i] = null;
							}
						}
						ResultSet first = c.getMetaData().getTables(params[1], params[2], params[3], null);

						ArrayList<ArrayList<String>> schema = new ArrayList<ArrayList<String>>();
						for (int i = 1; i < first.getMetaData().getColumnCount() + 1; i++) {
							ArrayList<String> nextCouple = new ArrayList<String>();
							nextCouple.add(first.getMetaData().getColumnName(i).toUpperCase());
							nextCouple.add(first.getMetaData().getColumnTypeName(i));
							schema.add(nextCouple);
						}
						/*
						 * ArrayList<String> typenames=new ArrayList<String>();
						 * ArrayList<String> names=new ArrayList<String>();
						 * for(int
						 * i=1;i<first.getMetaData().getColumnCount()+1;i++){
						 * names.add(first.getMetaData().getColumnName(i));
						 * typenames
						 * .add(first.getMetaData().getColumnTypeName(i)); }
						 * schema.add(names); schema.add(typenames);
						 */

						HashMap<String, ArrayList<ArrayList<String>>> h = new HashMap<String, ArrayList<ArrayList<String>>>();
						h.put("schema", schema);
						h.put("errors", new ArrayList<ArrayList<String>>());
						Gson g = new Gson();
						StringBuilder sb = new StringBuilder();
						sb.append(g.toJson(h, h.getClass()));
						while (first.next()) {
							if (first.getString(3).startsWith("table")) {
								// ignore temporary tables
								continue;
							}
							sb.append("\n");
							ArrayList<Object> res = new ArrayList<Object>();
							for (int i = 1; i < first.getMetaData().getColumnCount() + 1; i++) {
								res.add(first.getObject(i));

							}
							sb.append(g.toJson((ArrayList<Object>) res));

						}
						first.close();
						// stmt.close();
						c.close();
						log.error("Setting response for Tables:" + sb.toString());
						InputStreamEntity se = new InputStreamEntity(new ByteArrayInputStream(sb.toString().getBytes()),
								-1, ContentType.TEXT_PLAIN);
						httpResponse.setEntity(se);
					} else if (query.startsWith("getIndexInfo")) {
						Class.forName("org.sqlite.JDBC");
						String path = dbname;
						if (!path.endsWith("/")) {
							path += "/";
						}

						Connection c = createDummyDB(path);

						// Connection c = cons.get(path);
						// Connection c = DriverManager
						// .getConnection("jdbc:sqlite:" + path+"registry.db");
						String[] params = query.split(" ");
						for (int i = 1; i < params.length; i++) {
							if (params[i].equalsIgnoreCase("null")) {
								params[i] = null;
							}
						}
						ResultSet first = c.getMetaData().getIndexInfo(params[1], null, params[3],
								Boolean.parseBoolean(params[4]), Boolean.parseBoolean(params[5]));

						ArrayList<ArrayList<String>> schema = new ArrayList<ArrayList<String>>();
						for (int i = 1; i < first.getMetaData().getColumnCount() + 1; i++) {
							ArrayList<String> nextCouple = new ArrayList<String>();
							nextCouple.add(first.getMetaData().getColumnName(i).toUpperCase());
							nextCouple.add(first.getMetaData().getColumnTypeName(i));
							schema.add(nextCouple);
						}
						/*
						 * ArrayList<String> typenames=new ArrayList<String>();
						 * ArrayList<String> names=new ArrayList<String>();
						 * for(int
						 * i=1;i<first.getMetaData().getColumnCount()+1;i++){
						 * names.add(first.getMetaData().getColumnName(i));
						 * typenames
						 * .add(first.getMetaData().getColumnTypeName(i)); }
						 * schema.add(names); schema.add(typenames);
						 */

						HashMap<String, ArrayList<ArrayList<String>>> h = new HashMap<String, ArrayList<ArrayList<String>>>();
						h.put("schema", schema);
						h.put("errors", new ArrayList<ArrayList<String>>());
						Gson g = new Gson();
						StringBuilder sb = new StringBuilder();
						sb.append(g.toJson(h, h.getClass()));
						while (first.next()) {
							sb.append("\n");
							ArrayList<Object> res = new ArrayList<Object>();
							for (int i = 1; i < first.getMetaData().getColumnCount() + 1; i++) {
								res.add(first.getObject(i));

							}
							sb.append(g.toJson((ArrayList<Object>) res));

						}
						first.close();
						// stmt.close();
						c.close();

						InputStreamEntity se = new InputStreamEntity(new ByteArrayInputStream(sb.toString().getBytes()),
								-1, ContentType.TEXT_PLAIN);
						httpResponse.setEntity(se);
					}else if (query.startsWith("getPrimaryKeys")) {
						Class.forName("org.sqlite.JDBC");
						String path = dbname;
						if (!path.endsWith("/")) {
							path += "/";
						}

						Connection c = createDummyDB(path);

						// Connection c = cons.get(path);
						// Connection c = DriverManager
						// .getConnection("jdbc:sqlite:" + path+"registry.db");
						String[] params = query.split(" ");
						System.out.println("params:"+params);
						for (int i = 1; i < params.length; i++) {
							if (params[i].equalsIgnoreCase("null")) {
								params[i] = null;
							}
						}
						ResultSet first = c.getMetaData().getPrimaryKeys(params[1], null, params[3]);

						ArrayList<ArrayList<String>> schema = new ArrayList<ArrayList<String>>();
						for (int i = 1; i < first.getMetaData().getColumnCount() + 1; i++) {
							ArrayList<String> nextCouple = new ArrayList<String>();
							nextCouple.add(first.getMetaData().getColumnName(i).toUpperCase());
							nextCouple.add(first.getMetaData().getColumnTypeName(i));
							schema.add(nextCouple);
						}
						
						HashMap<String, ArrayList<ArrayList<String>>> h = new HashMap<String, ArrayList<ArrayList<String>>>();
						h.put("schema", schema);
						h.put("errors", new ArrayList<ArrayList<String>>());
						Gson g = new Gson();
						StringBuilder sb = new StringBuilder();
						sb.append(g.toJson(h, h.getClass()));
						while (first.next()) {
							sb.append("\n");
							ArrayList<Object> res = new ArrayList<Object>();
							for (int i = 1; i < first.getMetaData().getColumnCount() + 1; i++) {
								if(i==5){
									//sqlite jdbc bug, starts seq from 0, should be 1
									int seq=first.getInt(5);
									res.add(seq+1);
								}
								else{
									res.add(first.getObject(i));
								}
							}
							sb.append(g.toJson((ArrayList<Object>) res));

						}
						first.close();
						// stmt.close();
						c.close();

						InputStreamEntity se = new InputStreamEntity(new ByteArrayInputStream(sb.toString().getBytes()),
								-1, ContentType.TEXT_PLAIN);
						httpResponse.setEntity(se);
					} 
					else if (query.startsWith("getColumns")) {
						Class.forName("org.sqlite.JDBC");
						String path = dbname;
						if (!path.endsWith("/")) {
							path += "/";
						}

						Connection c = createDummyDB(path);
						log.debug("con to dummy db:" + c);
						// Connection c = DriverManager
						// .getConnection("jdbc:sqlite:" + path+"registry.db");
						String[] params = query.split(" ");
						for (int i = 1; i < params.length; i++) {
							if (params[i].equalsIgnoreCase("null")) {
								params[i] = null;
							}
						}
						ResultSet first = c.getMetaData().getColumns(params[1], params[2], params[3], params[4]);
						// log.error(params[1]+ params[2]+ params[3]+
						// params[4]);
						ArrayList<ArrayList<String>> schema = new ArrayList<ArrayList<String>>();
						for (int i = 1; i < first.getMetaData().getColumnCount() + 1; i++) {
							// log.error(first.getMetaData().getColumnName(i));
							ArrayList<String> nextCouple = new ArrayList<String>();
							nextCouple.add(first.getMetaData().getColumnName(i).toUpperCase());
							nextCouple.add(first.getMetaData().getColumnTypeName(i));
							schema.add(nextCouple);
						}
						/*
						 * ArrayList<String> typenames=new ArrayList<String>();
						 * ArrayList<String> names=new ArrayList<String>();
						 * for(int
						 * i=1;i<first.getMetaData().getColumnCount()+1;i++){
						 * names.add(first.getMetaData().getColumnName(i));
						 * typenames
						 * .add(first.getMetaData().getColumnTypeName(i)); }
						 * schema.add(names); schema.add(typenames);
						 */

						HashMap<String, ArrayList<ArrayList<String>>> h = new HashMap<String, ArrayList<ArrayList<String>>>();
						h.put("schema", schema);
						h.put("errors", new ArrayList<ArrayList<String>>());
						Gson g = new Gson();
						StringBuilder sb = new StringBuilder();
						sb.append(g.toJson(h, h.getClass()));

						while (first.next()) {
							sb.append("\n");
							ArrayList<Object> res = new ArrayList<Object>();
							for (int i = 1; i < first.getMetaData().getColumnCount() + 1; i++) {
								// log.error(first.getObject(i));
								res.add(first.getObject(i));

							}
							sb.append(g.toJson((ArrayList<Object>) res));

						}
						first.close();
						// stmt.close();
						c.close();
						// log.error("Setting response for Coluns:" +
						// sb.toString());
						InputStreamEntity se = new InputStreamEntity(new ByteArrayInputStream(sb.toString().getBytes()),
								-1, ContentType.TEXT_PLAIN);
						httpResponse.setEntity(se);
					} else if (query.startsWith("explain") || query.startsWith("EXPLAIN")) {
						String path = dbname;
						if (!path.endsWith("/")) {
							path += "/";
						}
						NodeSelectivityEstimator nse = null;
						try {
							nse = new NodeSelectivityEstimator(path + "histograms.json");
						} catch (Exception e) {
							log.error("Cannot read statistics. " + e.getMessage());
						}
						List<SQLQuery> subqueries = new ArrayList<SQLQuery>();
						SQLQuery squery;
						try {
							log.debug("Parsing SQL Query ...");
							NodeHashValues hashes = new NodeHashValues();
							hashes.setSelectivityEstimator(nse);
							Map<String, Set<String>> refCols=new HashMap<String, Set<String>>();
							squery = SQLQueryParser.parse(query.substring(8, query.length()), hashes, n2a, refCols);
							QueryDecomposer d = new QueryDecomposer(squery, path, workers, hashes);
							d.addRefCols(refCols);
							d.setN2a(n2a);
							log.debug("SQL Query Decomposing ...");
							log.debug("Number of workers: " + workers);
							d.setImportExternal(false);
							subqueries = d.getSubqueries();
							nse = null;
							hashes = null;
							ArrayList<ArrayList<String>> schema = new ArrayList<ArrayList<String>>();
							// ArrayList<String> typenames=new
							// ArrayList<String>();
							// typenames.add("VARCHAR");
							// schema.add(typenames);
							ArrayList<String> names = new ArrayList<String>();
							names.add("RESULT");
							names.add("VARCHAR");
							schema.add(names);
							HashMap<String, ArrayList<ArrayList<String>>> h = new HashMap<String, ArrayList<ArrayList<String>>>();
							h.put("schema", schema);
							h.put("errors", new ArrayList<ArrayList<String>>());
							Gson g = new Gson();
							StringBuilder sb = new StringBuilder();
							sb.append(g.toJson(h, h.getClass()));

							HashMap<String, byte[]> hashIDMap = new HashMap<>();
							for (SQLQuery q : subqueries) {
								hashIDMap.put(q.getResultTableName(), q.getHashId().asBytes());
								sb.append("\n");
								ArrayList<Object> res = new ArrayList<Object>();
								if (q.isFederated()) {
									q.removePasswordFromMadis();
								}
								res.add(q.toDistSQL());
								sb.append(g.toJson((ArrayList<Object>) res));
							}
							httpResponse.setEntity(
									new InputStreamEntity(new ByteArrayInputStream(sb.toString().getBytes())));
						} catch (Exception e) {
							log.error(e);
							httpResponse.setStatusCode(500);
							if (e.getMessage() == null) {
								httpResponse
										.setEntity(new StringEntity("error explaining query", ContentType.TEXT_PLAIN));
							} else {
								httpResponse.setEntity(new StringEntity(e.getMessage(), ContentType.TEXT_PLAIN));
							}

						}

					} else if (query.startsWith("analyze table")) {
						String[] t = query.replace("analyze table ", "").split(" ");
						if (t.length == 0) {
							log.warn("Cannot analyze table, no columns given");
							InputStreamEntity se = new InputStreamEntity(createOKResultStream(), -1,
									ContentType.TEXT_PLAIN);

							httpResponse.setEntity(se);
						} else {
							Connection conn = null;
							try {
								String path = dbname;
								if (!path.endsWith("/")) {
									path += "/";
								}

								DBInfoReaderDB.read(path);
								String tablename = t[0];
								if(tablename.startsWith("adp.")){
									tablename=tablename.substring(4);
								}
								Table tab = new Table(tablename, tablename);

                                Set<String> attrs = new HashSet<String>();
                                for (int i = 1; i < t.length; i++) {
                                    attrs.add(t[i]);
                                }
                                if (!tab.isFederated()) {
                                    //trying to analyze internal table
                                    log.debug("trying to analyze internal table: "+tab.getName());
                                    log.debug("will try to analyze only 0 partition");
                                    //TODO distributed analyzer
                                    conn = DriverManager.getConnection("jdbc:sqlite:" +path + tablename + ".0.db");
                                    ExternalAnalyzer fa = new ExternalAnalyzer(dbname, conn, "main");

                                    Schema sch = fa.analyzeAttrs(tablename, attrs);
                                    // change table name back to adding DB id
                                    log.debug("Saving stats to file");
                                    StatUtils.addSchemaToFile(path + "histograms.json", sch);
                                    conn.close();
								} else {
									String endpointID = tab.getDBName();
									String localTblName = tablename.substring(endpointID.length() + 1);
									DB db = DBInfoReaderDB.dbInfo.getDB(endpointID);

									Class.forName(db.getDriver());
									conn = DriverManager.getConnection(db.getURL(), db.getUser(), db.getPass());
									ExternalAnalyzer fa = new ExternalAnalyzer(dbname, conn, db.getSchema());

									Schema sch = fa.analyzeAttrs(localTblName, attrs);
									// change table name back to adding DB id
									log.debug("Saving stats to file");
									sch.getTableIndex().put(tablename, sch.getTableIndex().get(localTblName));
									sch.getTableIndex().remove(localTblName);
									StatUtils.addSchemaToFile(path + "histograms.json", sch);
									conn.close();
								}
								InputStreamEntity se = new InputStreamEntity(createOKResultStream(), -1,
										ContentType.TEXT_PLAIN);

								log.debug("Sending OK : " + se.toString());
								httpResponse.setEntity(se);
							} catch (Exception e) {
								log.error(e);
								if (conn != null) {
									conn.close();
								}
								httpResponse.setStatusCode(500);
								httpResponse.setEntity(new StringEntity(e.getMessage(), ContentType.TEXT_PLAIN));
							}
						}
					} else if (query.equals("select 1")) {

						// temporary fix to avoid executing onto select 1
						// queries
						InputStreamEntity se = new InputStreamEntity(create1ResultStream(), -1, ContentType.TEXT_PLAIN);
						log.debug("Sending 1 : " + se.toString());
						httpResponse.setEntity(se);
					} else if (!query.startsWith("distributed")) {
                        boolean send1=false;
						int timeoutMs = 0;
						try {
							timeoutMs = Integer.parseInt(inputContent.get(ExaremeGatewayUtils.REQUEST_TIMEOUT)) * 1000;
						} catch (NumberFormatException e) {
							log.error("Timeout not an integer:" + ExaremeGatewayUtils.REQUEST_TIMEOUT);
							log.error(inputContent.toString());
						}
						long start = System.currentTimeMillis();
						String path = dbname;
						if (!path.endsWith("/")) {
							path += "/";
						}
						NodeSelectivityEstimator nse = null;
						try {
							nse = new NodeSelectivityEstimator(path + "histograms.json");
						} catch (Exception e) {
							log.error("Cannot read statistics. " + e.getMessage());
						}
						List<SQLQuery> subqueries = new ArrayList<SQLQuery>();
						SQLQuery squery;
						try {
							log.debug("Parsing SQL Query ...");
							NodeHashValues hashes = new NodeHashValues();
							hashes.setSelectivityEstimator(nse);
							Map<String, Set<String>> refCols=new HashMap<String, Set<String>>();
							if (DecomposerUtils.WRITE_ALIASES) {
								n2a = DBInfoReaderDB.readAliases(path);
							}

                            AdpDBClientProperties props = new AdpDBClientProperties(dbname, "", "", useCache, false,
                                    false, false, -1, 10, null);
                            AdpDBClient dbClient = AdpDBClientFactory.createDBClient(manager, props);
                            AdpDBClientQueryStatus status = null;

                            Map<String, String> extraCommands = new HashMap<String, String>();
                            String decomposedQuery = "";
                            String resultTblName = "";
                            String finalQuery = null;
                            Set<String> referencedTables = new HashSet<String>();
                            boolean a = false;

							squery = SQLQueryParser.parse(query, hashes, n2a, refCols);

                            if(squery.isDrop()){
                                String drop="distributed drop table "+squery.getTemporaryTableName()+";";
                                status = dbClient.query("dquery", drop, extraCommands);
                                send1=true;
                            }
                            else{
                                QueryDecomposer d = new QueryDecomposer(squery, path, workers, hashes, useCache);
                                d.addRefCols(refCols);
                                d.setN2a(n2a);
                                log.debug("n2a:" + n2a.toString());
                                log.debug("SQL Query Decomposing ...");
                                log.debug("Number of workers: " + workers);
                                subqueries = d.getSubqueries();

                                if (DecomposerUtils.WRITE_ALIASES) {
                                    DBInfoWriterDB.writeAliases(n2a, path);
                                }
                                squery = null;
                                d = null;
                                nse = null;
                                hashes = null;
                                

                                HashMap<String, Pair<byte[], String>> hashQueryMap = new HashMap<>();
                                if (a) {
                                    SQLQuery last = subqueries.get(subqueries.size() - 1);
                                    finalQuery = last.toSQL();
                                    for (Table t : last.getAllAttachedTables()) {
                                        referencedTables.add(t.getName());
                                    }

                                }
                                if((timeoutMs>0&& System.currentTimeMillis() - start > timeoutMs)||serverCon.getStatus()==NHttpConnection.CLOSED){
                                	log.debug("query time out or cancel");
                                	resultTblName=null;
                                }
                                else if (subqueries.size() == 1 && subqueries.get(0).existsInCache()) {
                                    resultTblName = subqueries.get(0).getInputTables().get(0).getAlias();
                                    //System.out.println("namesss "+resultTblName);
                                    Cache cache = new Cache(props);
                                    List<madgik.exareme.common.schema.Table> tables = new ArrayList<>(1);
                                    tables.add(new madgik.exareme.common.schema.Table(resultTblName));
                                    cache.updateCacheForTableUse(tables);

                                } else if(subqueries.size() == 1 && subqueries.get(0).isSelectAllFromInternal()) {
                                    resultTblName = subqueries.get(0).getInputTables().get(0).getName();
                                }
                                else{

                                    for (SQLQuery q : subqueries) {
                                        if (a && !q.isTemporary()) {
                                            continue;
                                            // don't add last table
                                        }
                                        if (referencedTables.contains(q.getTemporaryTableName())) {
                                            q.setTemporary(false);
                                        }
                                        String ptnCol = null;
                                        if (q.getPartitionColumn() != null) {
                                            ptnCol = q.getPartitionColumn().toString();
                                        }
                                        try {
                                            hashQueryMap.put(q.getResultTableName(),
                                                    new Pair(q.getHashId().asBytes(), ptnCol));
                                            //log.debug("giving hash id for: "+q.getResultTableName());
                                        } catch (NullPointerException we) {
                                            log.error("null hash id");
                                        }
                                        String dSQL = q.toDistSQL();
                                        decomposedQuery += dSQL + "\n\n";
                                        if (!q.isTemporary()) {
                                            resultTblName = q.getTemporaryTableName();
                                        }
                                        if (q.getCreateSipTables() != null) {
                                            extraCommands.put(q.getTemporaryTableName(), q.getCreateSipTables());
                                        }

                                    }
                                    log.debug("Decomposed Query : " + decomposedQuery);
                                    log.debug("Result Table : " + resultTblName);
                                    log.debug("Executing decomposed query ...");

                                    // when using cache
                                    if (useCache) {
                                        status = dbClient.query("dquery", decomposedQuery, hashQueryMap, extraCommands, subqueries);
                                    } else {
                                        status = dbClient.query("dquery", decomposedQuery, extraCommands);
                                    }
								// AdpDBClientQueryStatus status =
								// dbClient.query("dquery", decomposedQuery,
								// hashIDMap);

                                }
                            }
                            
                    		
                            if(status!=null){
								while (status.hasFinished() == false && status.hasError() == false) {
									//System.out.println("Status:::"+c.getStatus());
									if (timeoutMs > 0||serverCon.getStatus()==NHttpConnection.CLOSED) {
										long timePassed = System.currentTimeMillis() - start;
										
										if (timePassed > timeoutMs||serverCon.getStatus()==NHttpConnection.CLOSED) {
											
											//status.close();
											if(killPython){
												log.debug("killing python");
												String[] command = {"/bin/sh", "bin/killPython.sh"};
										        ProcessBuilder probuilder = new ProcessBuilder( command );

										        //You can set up your work directory
										        probuilder.directory(new File("/opt/exareme/ADPControl/"));
										        
										        Process process = probuilder.start();
										        
										        //Read out dir output
										        InputStream is = process.getInputStream();
										        InputStreamReader isr = new InputStreamReader(is);
										        BufferedReader br = new BufferedReader(isr);
										        String line;
										        while ((line = br.readLine()) != null) {
										            log.debug(line);
										        }
										        
										        //Wait to get exit value
										        try {
										            int exitValue = process.waitFor();
										            log.debug("exit value:"+exitValue);
										        } catch (InterruptedException e) {
										            // TODO Auto-generated catch block
										            e.printStackTrace();
										        }
											}
											log.warn("Time out:" + timeoutMs + " ms passed");
											throw new RuntimeException("Time out:" + timeoutMs + " ms passed");
										}
									}
									Thread.sleep(100);
								}
								if (status.hasError()) {
									throw new RuntimeException(status.getError());
								}
							}

							if (a) {
								HashMap<String, Object> additionalProps = new HashMap<String, Object>();
								additionalProps.put("time", -1);
								additionalProps.put("errors", new ArrayList<Object>());
								ExecutorService pool = Executors.newFixedThreadPool(1);
								PipedOutputStream out = new PipedOutputStream();
								pool.submit(new AdpDBQueryExecutorThread(finalQuery, additionalProps, props,
										referencedTables, out));
								log.debug("Net Reader submitted.");
								log.debug("Sending Result Table : " + resultTblName);
								httpResponse.setEntity(new InputStreamEntity(new PipedInputStream(out)));
							} else {
                                if(!send1){
                                    log.debug("Sending Result Table : " + resultTblName);
                                    httpResponse.setEntity(new InputStreamEntity(dbClient.readTable(resultTblName)));
                                }
                                else{
                                    InputStreamEntity se = new InputStreamEntity(create1ResultStream(), -1, ContentType.TEXT_PLAIN);
                                    log.debug("Sending 1 : " + se.toString());
                                    httpResponse.setEntity(se);
                                }
							}
						} catch (Exception e) {
							log.error("Error:", e);
							httpResponse.setStatusCode(500);
							if (e.getMessage() == null) {
								httpResponse
										.setEntity(new StringEntity("error executing query", ContentType.TEXT_PLAIN));
							} else {
								httpResponse.setEntity(new StringEntity(e.getMessage(), ContentType.TEXT_PLAIN));
							}

						}
					} else {
						log.error("Bad request");
						throw new RuntimeException("Bad Request");
					}
				} catch (Exception e) {
					// e.printStackTrace();
					log.error(e);
					httpResponse.setStatusCode(500);
					httpResponse.setEntity(new StringEntity(e.getMessage(), ContentType.TEXT_PLAIN));
				}
				httpExchange.submitResponse(new BasicAsyncResponseProducer(httpResponse));
			}
		}.start();
	}

	private InputStream createOKResultStream() {
		// HashMap<String, ArrayList<ArrayList<String>>> firstRow=new
		// HashMap<String, ArrayList<ArrayList<String>>>();
		ArrayList<ArrayList<String>> schema = new ArrayList<ArrayList<String>>();
		// ArrayList<String> typenames=new ArrayList<String>();
		// typenames.add("VARCHAR");
		// schema.add(typenames);
		ArrayList<String> names = new ArrayList<String>();
		names.add("RESULT");
		names.add("VARCHAR");
		schema.add(names);
		HashMap<String, ArrayList<ArrayList<String>>> h = new HashMap<String, ArrayList<ArrayList<String>>>();
		h.put("schema", schema);
		h.put("errors", new ArrayList<ArrayList<String>>());
		Gson g = new Gson();
		StringBuilder sb = new StringBuilder();
		sb.append(g.toJson(h, h.getClass()));
		sb.append("\n");
		ArrayList<Object> res = new ArrayList<Object>();
		res.add("OK");
		sb.append(g.toJson((ArrayList<Object>) res));
		return new ByteArrayInputStream(sb.toString().getBytes());
	}

	private InputStream create1ResultStream() {
		// HashMap<String, ArrayList<ArrayList<String>>> firstRow=new
		// HashMap<String, ArrayList<ArrayList<String>>>();
		ArrayList<ArrayList<String>> schema = new ArrayList<ArrayList<String>>();
		// ArrayList<String> typenames=new ArrayList<String>();
		// typenames.add("VARCHAR");
		// schema.add(typenames);
		ArrayList<String> names = new ArrayList<String>();
		names.add("RESULT");
		names.add("INTEGER");
		schema.add(names);
		HashMap<String, ArrayList<ArrayList<String>>> h = new HashMap<String, ArrayList<ArrayList<String>>>();
		h.put("schema", schema);
		h.put("errors", new ArrayList<ArrayList<String>>());
		Gson g = new Gson();
		StringBuilder sb = new StringBuilder();
		sb.append(g.toJson(h, h.getClass()));
		sb.append("\n");
		ArrayList<Object> res = new ArrayList<Object>();
		res.add(new Integer(1));
		sb.append(g.toJson((ArrayList<Object>) res));
		return new ByteArrayInputStream(sb.toString().getBytes());
	}

	private Connection createDummyDB(String path) {
		try {
			Class.forName("org.sqlite.JDBC");
            Connection c = DriverManager.getConnection("jdbc:sqlite:" + path
                                + "registry.db");
			Statement stmt = c.createStatement();
			ResultSet rs;
            Connection c2 = DriverManager.getConnection("jdbc:sqlite:" + path
                     					+ "dummy.db");
			try {
				rs = stmt.executeQuery("SELECT sql_definition FROM sql");
			} catch (Exception e) {
				log.debug("Could not read registry");
				stmt.close();
				c.close();
				return c2;
			}
			log.debug("getting sql definition");

			Statement statement = c2.createStatement();
			String create = "";
			while (rs.next()) {
				create = rs.getString(1);
                create = create.replace("CREATE TABLE",
                        						"CREATE TABLE IF NOT EXISTS");
				statement.executeUpdate(create);
			}
			rs.close();
			statement.close();
			stmt.close();
			c.close();
			return c2;
		} catch (Exception e) {
			e.printStackTrace();
			log.error(e);
		}
		return null;
	}

}

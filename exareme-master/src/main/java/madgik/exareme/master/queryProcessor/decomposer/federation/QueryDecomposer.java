/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package madgik.exareme.master.queryProcessor.decomposer.federation;

import madgik.exareme.common.schema.PhysicalTable;
import madgik.exareme.master.queryProcessor.decomposer.DecomposerUtils;
import madgik.exareme.master.queryProcessor.decomposer.dag.Node;
import madgik.exareme.master.queryProcessor.decomposer.dag.NodeHashValues;
import madgik.exareme.master.queryProcessor.decomposer.dag.PartitionCols;
import madgik.exareme.master.queryProcessor.decomposer.query.*;
import madgik.exareme.master.queryProcessor.decomposer.util.Util;
import madgik.exareme.master.queryProcessor.estimator.NodeCostEstimator;
import madgik.exareme.master.registry.Registry;
import madgik.exareme.utils.properties.AdpDBProperties;

import org.apache.log4j.Logger;

import com.google.common.hash.HashCode;

import java.io.File;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

//import di.madgik.statistics.tools.OverlapAnalyzer;

/**
 * @author dimitris
 */
public class QueryDecomposer {

	private static final Logger log = Logger.getLogger(QueryDecomposer.class);
	private SQLQuery initialQuery;
	ArrayList<SQLQuery> result;
	private int noOfparts;
	private Node root;
	private Node union;
	private NodeHashValues hashes;
	private final boolean multiOpt;
	private final boolean centralizedExecution;
	private NamesToAliases n2a;
	private boolean addNotNulls;
	private boolean projectRefCols;
	private String db;
	private SipStructure sipInfo;
	private Map<String, Set<String>> refCols;
	// private NodeSelectivityEstimator nse;
	private Map<Node, Double> limits;
	private boolean addAliases;
	private boolean importExternal;
	private boolean useSIP = false;
	// private Registry registry;
	private Map<HashCode, madgik.exareme.common.schema.Table> registry;
	private final boolean useCache = AdpDBProperties.getAdpDBProps().getBoolean("db.cache");
	private final int mostProminent = DecomposerUtils.MOST_PROMINENT;
	private final boolean useGreedy = DecomposerUtils.USE_GREEDY;
	private long getOnlyLeftDeepTreesTime= DecomposerUtils.EXPAND_DAG_TIME;
	private boolean onlyLeft = false;
	private int unionnumber;
	SipToUnions sipToUnions;
	private NodeCostEstimator nce;
	private long startTime;
	private int workers;

	public QueryDecomposer(SQLQuery initial) throws ClassNotFoundException {
		this(initial, ".", 1, null);
	}

	public QueryDecomposer(SQLQuery initial, String database, int noOfPartitions, NodeHashValues h) {
		result = new ArrayList<SQLQuery>();
		this.initialQuery = initial;
		this.noOfparts = noOfPartitions;
		workers=noOfPartitions;
		registry = new HashMap<HashCode, madgik.exareme.common.schema.Table>();
		for (PhysicalTable pt : Registry.getInstance(database).getPhysicalTables()) {
			byte[] hash = pt.getTable().getHashID();
			if (hash != null) {
				registry.put(HashCode.fromBytes(pt.getTable().getHashID()), pt.getTable());
			}
		}

		try {
			// read dbinfo from properties file
			DBInfoReaderDB.read(database);
		} catch (ClassNotFoundException ex) {
			java.util.logging.Logger.getLogger(QueryDecomposer.class.getName()).log(Level.SEVERE, null, ex);
		}
		this.db = database;
		// DBInfoReader.read("./conf/dbinfo.properties");
		union = new Node(Node.AND);
		if (initialQuery.isUnionAll()) {
			union.setObject(("UNIONALL"));
			union.setOperator(Node.UNIONALL);
			this.useSIP = false;
		} else {
			union.setObject(("UNION"));
			union.setOperator(Node.UNION);
		}

		root = new Node(Node.OR);
		root.setObject(new Table("table" + Util.createUniqueId(), null));
		root.addChild(union);
		if (initialQuery.getOrderBy().size() > 0) {
			Node orderBy = new Node(Node.AND, Node.ORDERBY);
			orderBy.addChild(root);
			List<Column> orderCols = new ArrayList<Column>();
			orderBy.setObject(orderCols);
			for (ColumnOrderBy ob : initialQuery.getOrderBy()) {
				orderCols.add(new ColumnOrderBy(null, ob.getName(), ob.isAsc));
			}
			Node orderByParent = new Node(Node.OR);
			orderByParent.addChild(orderBy);
			orderByParent.setObject(new Table("table" + Util.createUniqueId(), null));
			root = orderByParent;
		}

		// this.nse = nse;
		hashes = h;
		// hashes.setSelectivityEstimator(nse);
		this.projectRefCols = DecomposerUtils.PROJECT_REF_COLS;
		multiOpt = DecomposerUtils.MULTI;
		centralizedExecution = DecomposerUtils.CENTRALIZED;
		this.addAliases = DecomposerUtils.ADD_ALIASES;
		// this.n2a = new NamesToAliases();
		this.addNotNulls = DecomposerUtils.ADD_NOT_NULLS;
		// this.memo = new Memo();
		this.limits = new HashMap<Node, Double>();
		this.importExternal = DecomposerUtils.IMPORT_EXTERNAL;
		if (projectRefCols) {
			refCols = new HashMap<String, Set<String>>();
			initial.generateRefCols(refCols);
			/*
			 * for (Table t : initial.getAllReferencedTables()) { Set<String>
			 * colsForT = new HashSet<String>();
			 * if(refCols.containsKey(t.getName())){
			 * colsForT=refCols.get(t.getName()); } for (Column c :
			 * initial.getAllReferencedColumns()) { if
			 * (t.getAlias().equals(c.tableAlias)) { colsForT.add(c.columnName);
			 * } } refCols.put(t.getName(), colsForT); }
			 */
		}
		if (useSIP) {
			sipInfo = new SipStructure();
		}

		nce = new NodeCostEstimator(this.noOfparts);
	}

	public List<SQLQuery> getSubqueries() throws Exception {
		initialQuery.normalizeWhereConditions();
		if (initialQuery.hasNestedSuqueries()) {
			if (!this.multiOpt && !initialQuery.getUnionqueries().isEmpty()) {
				List<SQLQuery> res = new ArrayList<SQLQuery>();
				SQLQuery finalUnion = new SQLQuery();
				for (SQLQuery u : initialQuery.getUnionqueries()) {
					QueryDecomposer d = new QueryDecomposer(u, this.db, this.noOfparts, hashes);
					for (SQLQuery q2 : d.getSubqueries()) {
						res.add(q2);
						if (!q2.isTemporary()) {
							finalUnion.getUnionqueries().add(q2);
							q2.setTemporary(true);
						}
					}
				}
				finalUnion.setTemporary(false);
				res.add(finalUnion);
				return res;
			}
			initialQuery.setTemporary(false);
			decomposeSubquery(initialQuery);
		} else {
			List<List<String>> aliases = initialQuery.getListOfAliases(n2a, true);
			// for(List<String> aliases:initialQuery.getListOfAliases(n2a)){
			List<String> firstAliases = aliases.get(0);
			initialQuery.renameTables(firstAliases);
			ConjunctiveQueryDecomposer d = new ConjunctiveQueryDecomposer(initialQuery, centralizedExecution,
					addNotNulls);
			Node topSubquery = d.addCQToDAG(union, hashes);
			// String u=union.dotPrint();
			if (addAliases) {
				for (int i = 1; i < aliases.size(); i++) {
					List<String> nextAliases = aliases.get(i);
					topSubquery.addChild(addAliasesToDAG(topSubquery, firstAliases, nextAliases, hashes));
				}
			}

		}

		// root.setIsCentralised(union.isCentralised());
		// root.setPartitionedOn(union.isPartitionedOn());
		List<SQLQuery> res = getPlan();
		log.debug("Plan generated");
		for (int i = 0; i < res.size(); i++) {
			SQLQuery s = res.get(i);
			log.debug("refactoring " + s.getTemporaryTableName() + " for federation...");
			s.refactorForFederation();
			if (i == res.size() - 1) {
				s.setTemporary(false);
			}
			if (s.isFederated() && !this.initialQuery.isUnionAll() && DecomposerUtils.PUSH_DISTINCT) {
				s.setOutputColumnsDinstict(true);
			}
		}

		if (importExternal) {
			// create dir to store dbs if not exists
			File theDir = new File(this.db + "import/");

			// if the directory does not exist, create it
			if (!theDir.exists()) {
				log.debug("creating directory: " + this.db + "import/");
				boolean result = false;

				try {
					theDir.mkdir();
					result = true;
				} catch (SecurityException se) {
					log.error("Could not create dir to store external imports:" + se.getMessage());
				}
				if (result) {
					log.debug("Created dir to store external imports");
				}
			}
			ExecutorService es = Executors.newFixedThreadPool(6);

			for (int i = 0; i < res.size(); i++) {
				SQLQuery s = res.get(i);
				if (s.isFederated()) {
					// DecomposerUtils.ADD_TO_REGISTRY;
					boolean addToregistry =noOfparts == 1 && DecomposerUtils.ADD_TO_REGISTRY;
					DB dbinfo = DBInfoReaderDB.dbInfo.getDBForMadis(s.getMadisFunctionString());
					StringBuilder createTableSQL = new StringBuilder();
					if (dbinfo == null) {
						log.error("Could not import Data. DB not found:" + s.getMadisFunctionString());
						// return;
					}
					DataImporter di = new DataImporter(s, this.db, dbinfo);
					di.setAddToRegisrty(addToregistry);
					es.execute(di);
					if (addToregistry) {
						res.remove(i);
						i--;
						if (res.isEmpty()) {
							SQLQuery selectstar = new SQLQuery();
							selectstar.addInputTable(new Table(s.getTemporaryTableName(), s.getTemporaryTableName()));
							selectstar.setExistsInCache(true);
							res.add(selectstar);
							i++;
						}
					}
				}
			}
			es.shutdown();
			boolean finished = es.awaitTermination(160, TimeUnit.MINUTES);

		}
		return res;
	}

	public List<SQLQuery> getPlan() {
		// String dot0 = root.dotPrint();

		if (projectRefCols) {
			createProjections(root);
			log.debug("Base projections created");
		}
		//StringBuilder a = root.dotPrint(new HashSet<Node>());
		//System.out.println(a.toString());
		// long b=System.currentTimeMillis();
		unionnumber = 0;
		sipToUnions = new SipToUnions();
		sipToUnions.put(unionnumber, new HashSet<SipNode>());
		startTime=System.currentTimeMillis();
		//System.out.println("result cardinality::"+root.getChildAt(0).getChildAt(0).getNodeInfo().getNumberOfTuples());
		expandDAG(root);
		log.debug("DAG expanded");
		//System.out.println("expandtime:"+(System.currentTimeMillis()-b));
		//System.out.println("noOfnode:"+root.count(0));
		if (this.useSIP) {
			sipInfo.removeNotNeededSIPs();
		}
		//Set<Node> visited=new HashSet<Node>(new HashSet<Node>());
		//StringBuilder a2 = root.dotPrint(new HashSet<Node>());
		 //System.out.println(a2.toString());
		if(hashes.containsRangeJoin()){
			this.noOfparts=1;
		}
		 //System.out.println(root.dotPrint());
		// int no=root.count(0);
		if (this.initialQuery.getLimit() > -1) {
			Node limit = new Node(Node.AND, Node.LIMIT);
			limit.setObject(new Integer(this.initialQuery.getLimit()));
			limit.addChild(root);

			if (!hashes.containsKey(limit.getHashId())) {
				hashes.put(limit.getHashId(), limit);
				limit.addAllDescendantBaseTables(root.getDescendantBaseTables());
			} else {
				limit = hashes.get(limit.getHashId());
			}

			Node limitTable = new Node(Node.OR);
			limitTable.setObject(new Table("table" + Util.createUniqueId(), null));
			limitTable.addChild(limit);

			if (!hashes.containsKey(limitTable.getHashId())) {
				hashes.put(limitTable.getHashId(), limitTable);
				limitTable.addAllDescendantBaseTables(limit.getDescendantBaseTables());
			} else {
				limitTable = hashes.get(limitTable.getHashId());
			}
			root = limitTable;
		}
		// String a = root.dotPrint();
		long t1 = System.currentTimeMillis();
		
		SinglePlan best;
		Memo finalMemo = new Memo();
		double finalCost = 0;
		List<Node> shareable = new ArrayList<Node>();
		Map<Node, Double> greedyToMat = new HashMap<Node, Double>();
		if (this.noOfparts==1||DecomposerUtils.CHOOSE_MODE) {
			System.out.println("starting...");
			root.addShareable(shareable);
			System.out.println(shareable.size());
			// add top union results
			Node u1 = root.getChildAt(0);
			if (u1.getOpCode() == Node.UNION) {
				for (Node u : u1.getChildren()) {
					greedyToMat.put(u, 0.0);
				}
			} else {
				Node u2 = u1.getChildAt(0).getChildAt(0);
				if (u2.getOpCode() == Node.UNION) {
					for (Node u : u2.getChildren()) {
						greedyToMat.put(u, 0.0);
					}
				}
			}
			
				Collections.sort(shareable);

				
				unionnumber = 0;
				System.out.println("searching centralized plan...");
				nce.setPartitionNo(1);
				best = getBestPlanCentralized(root, Double.MAX_VALUE, finalMemo, greedyToMat);
				finalCost = best.getCost();
				System.out.println("found with cost "+finalCost);
				
		}
		if (this.noOfparts==1){
			
			System.out.println("no mat:" + finalCost);
				boolean existsBetterPlan = true;
				int indexOfBest = -1;
				while (existsBetterPlan) {
					if (indexOfBest > -1) {
						greedyToMat.put(shareable.get(indexOfBest), 0.0);
					}
					existsBetterPlan = false;

					for (int i = 0; i < mostProminent && i < shareable.size(); i++) {
						if (greedyToMat.containsKey(shareable.get(shareable.size() - (i + 1)))) {
							continue;
						}
						greedyToMat.put(shareable.get(shareable.size() - (i + 1)), 0.0);
						Memo memo = new Memo();

						if (noOfparts == 1) {
							if (this.useSIP) {
								this.sipInfo.resetCounters();
							}
							unionnumber = 0;
							best = getBestPlanCentralized(root, Double.MAX_VALUE, memo, greedyToMat);
							double matCost = 0.0;
							for (Double d : greedyToMat.values()) {
								matCost += d;
							}
							best.setCost(best.getCost() + matCost);
							System.out.println(shareable.get(shareable.size() - (i + 1)).getObject().toString());
							System.out.println(best.getCost());
							System.out.println("size:"+(shareable.get(shareable.size() - (i + 1)).getNodeInfo().getNumberOfTuples()));
							greedyToMat.remove(shareable.get(shareable.size() - (i + 1)));
							if (best.getCost() < finalCost) {
								indexOfBest = shareable.size() - (i + 1);
								finalCost = best.getCost();
								finalMemo = memo;
								existsBetterPlan = true;
							}
						}
					}
				}

			
		}
		else if(DecomposerUtils.CHOOSE_MODE){
			System.out.println("choosing mode...");
			if(Util.planContainsLargerResult(root, finalMemo, DecomposerUtils.DISTRIBUTED_LIMIT)){
				System.out.println("distributed...");
				nce.setPartitionNo(this.noOfparts);
				finalMemo=new Memo();
				best = getBestPlanPruned(root, null, Double.MAX_VALUE, Double.MAX_VALUE, new EquivalentColumnClasses(),
						new HashSet<MemoKey>(), finalMemo);
			}
			else{
				System.out.println("centralised...");
				this.noOfparts=1;
				
				System.out.println("no mat:" + finalCost);
					boolean existsBetterPlan = true;
					int indexOfBest = -1;
					while (existsBetterPlan) {
						if (indexOfBest > -1) {
							greedyToMat.put(shareable.get(indexOfBest), 0.0);
						}
						existsBetterPlan = false;

						for (int i = 0; i < mostProminent && i < shareable.size(); i++) {
							if (greedyToMat.containsKey(shareable.get(shareable.size() - (i + 1)))) {
								continue;
							}
							greedyToMat.put(shareable.get(shareable.size() - (i + 1)), 0.0);
							Memo memo = new Memo();

							if (noOfparts == 1) {
								if (this.useSIP) {
									this.sipInfo.resetCounters();
								}
								unionnumber = 0;
								best = getBestPlanCentralized(root, Double.MAX_VALUE, memo, greedyToMat);
								double matCost = 0.0;
								for (Double d : greedyToMat.values()) {
									matCost += d;
								}
								best.setCost(best.getCost() + matCost);
								System.out.println(shareable.get(shareable.size() - (i + 1)).getObject().toString());
								System.out.println(best.getCost());
								//System.out.println("size:"+(shareable.get(shareable.size() - (i + 1)).getNodeInfo().getNumberOfTuples()));
								greedyToMat.remove(shareable.get(shareable.size() - (i + 1)));
								if (best.getCost() < finalCost) {
									indexOfBest = shareable.size() - (i + 1);
									finalCost = best.getCost();
									finalMemo = memo;
									existsBetterPlan = true;
								}
							}
						}
					}
			}
			
		}
		else{
			//distributed
			finalMemo=new Memo();
			best = getBestPlanPruned(root, null, Double.MAX_VALUE, Double.MAX_VALUE, new EquivalentColumnClasses(),
					new HashSet<MemoKey>(), finalMemo);
		}

		System.out.println(System.currentTimeMillis() - t1);
		
		System.out.println("no of mat.:" + greedyToMat.size());
		
		

		// Plan best = addRepartitionAndComputeBestPlan(root, cost, memo, cel,
		// null);
		System.out.println(System.currentTimeMillis() - t1);
		//System.out.println("best cost:" + best.getCost());
		// System.out.println(memo.size());
		// String dot2 = root.dotPrint();
		// System.out.println(t1);
		//
		// Plan best = findBestPlan(root, cost, memo, new HashSet<Node>(), cel);
		// System.out.println(best.getPath().toString());
		SinlgePlanDFLGenerator dsql = new SinlgePlanDFLGenerator(root, noOfparts, finalMemo, registry);
		dsql.setN2a(n2a);
		if (this.useSIP) {
			dsql.setSipStruct(this.sipInfo);
			dsql.setUseSIP(useSIP);
			dsql.setSipToUnions(sipToUnions);
		}
		return (List<SQLQuery>) dsql.generate();
		// return null;
	}

	private void decomposeSubquery(SQLQuery s) throws Exception {
		// s.normalizeWhereConditions();
		for (SQLQuery u : s.getUnionqueries()) {

			// push limit
			if (s.getLimit() > -1) {
				if (u.getLimit() == -1) {
					u.setLimit(s.getLimit());
				} else {
					if (s.getLimit() < u.getLimit()) {
						u.setLimit(s.getLimit());
					}
				}
			}
			u.normalizeWhereConditions();
			if (u.hasNestedSuqueries()) {
				decomposeSubquery(u);
			} else {

				/*
				 * for (List<String> aliases : u.getListOfAliases(n2a)) {
				 * u.renameTables(aliases); ConjunctiveQueryDecomposer d = new
				 * ConjunctiveQueryDecomposer(u, centralizedExecution,
				 * addNotNulls); d.addCQToDAG(union, hashes); }
				 */

				List<List<String>> aliases = u.getListOfAliases(n2a, true);
				// for(List<String>
				// aliases:initialQuery.getListOfAliases(n2a)){
				List<String> firstAliases = aliases.get(0);
				u.renameTables(firstAliases);
				ConjunctiveQueryDecomposer d = new ConjunctiveQueryDecomposer(u, centralizedExecution, addNotNulls);
				Node topSubquery = d.addCQToDAG(union, hashes);
				// String u=union.dotPrint();
				if (addAliases) {
					for (int i = 1; i < aliases.size(); i++) {
						List<String> nextAliases = aliases.get(i);
						topSubquery.addChild(addAliasesToDAG(topSubquery, firstAliases, nextAliases, hashes));
					}
				}

			}

		}

		if (s.isSelectAll() && s.getBinaryWhereConditions().isEmpty() && s.getUnaryWhereConditions().isEmpty()
				&& s.getGroupBy().isEmpty() && s.getOrderBy().isEmpty() && s.getNestedSelectSubqueries().size() == 1
				&& !s.getNestedSelectSubqueries().keySet().iterator().next().hasNestedSuqueries()) {
			SQLQuery nested = s.getNestedSubqueries().iterator().next();
			// push limit
			if (s.getLimit() > -1) {
				if (nested.getLimit() == -1) {
					nested.setLimit(s.getLimit());
				} else {
					if (s.getLimit() < nested.getLimit()) {
						nested.setLimit(s.getLimit());
					}
				}
			}
		}
		// Collection<SQLQuery> nestedSubs=s.getNestedSubqueries();
		if (!s.getNestedSubqueries().isEmpty()) {
			for (SQLQuery nested : s.getNestedSubqueries()) {
				addNestedToDAG(nested, s);
			}

			// if s is an "empty" select * do not add it and rename the nested
			// with the s table name??
			if (s.isSelectAll() && s.getBinaryWhereConditions().isEmpty() && s.getUnaryWhereConditions().isEmpty()
					&& s.getGroupBy().isEmpty() && s.getOrderBy().isEmpty() && s.getNestedSelectSubqueries().size() == 1
					&& !s.getNestedSelectSubqueries().keySet().iterator().next().hasNestedSuqueries()) {
				union.addChild(s.getNestedSelectSubqueries().keySet().iterator().next().getNestedNode());
			} else {
				// decompose s changing the nested from tables

				List<List<String>> aliases = s.getListOfAliases(n2a, true);
				// for(List<String>
				// aliases:initialQuery.getListOfAliases(n2a)){
				List<String> firstAliases = new ArrayList<String>();
				if (!aliases.isEmpty()) {
					firstAliases = aliases.get(0);
					s.renameTables(firstAliases);
				}
				ConjunctiveQueryDecomposer d = new ConjunctiveQueryDecomposer(s, centralizedExecution, addNotNulls);
				Node topSubquery = d.addCQToDAG(union, hashes);
				// String u=union.dotPrint();
				if (addAliases) {
					for (int i = 1; i < aliases.size(); i++) {
						List<String> nextAliases = aliases.get(i);
						topSubquery.addChild(addAliasesToDAG(topSubquery, firstAliases, nextAliases, hashes));
					}
				}

			}

		}

	}

	public void addNestedToDAG(SQLQuery nested, SQLQuery parent) throws Exception {
		nested.normalizeWhereConditions();
		if (nested.hasNestedSuqueries()) {
			decomposeSubquery(nested);
		} else {

			// rename outputs
			if (!(parent.isSelectAll() && parent.getBinaryWhereConditions().isEmpty()
					&& parent.getUnaryWhereConditions().isEmpty() && parent.getNestedSelectSubqueries().size() == 1
					&& !parent.getNestedSelectSubqueries().keySet().iterator().next().hasNestedSuqueries())) {
				// rename outputs
				String alias = parent.getNestedSubqueryAlias(nested);
				for (Output o : nested.getOutputs()) {
					String name = o.getOutputName();
					o.setOutputName(alias + "_" + name);
				}
			}

			Node nestedNodeOr = new Node(Node.AND, Node.NESTED);
			Node nestedNode = new Node(Node.OR);
			nestedNode.setObject(new Table("table" + Util.createUniqueId().toString(), null));
			nestedNode.addChild(nestedNodeOr);
			nestedNodeOr.setObject(parent.getNestedSubqueryAlias(nested));
			nestedNode.addDescendantBaseTable(parent.getNestedSubqueryAlias(nested));
			/*
			 * for (List<String> aliases : nested.getListOfAliases(n2a))
			 * { nested.renameTables(aliases);
			 * ConjunctiveQueryDecomposer d = new
			 * ConjunctiveQueryDecomposer(nested, centralizedExecution,
			 * addNotNulls); d.addCQToDAG(union, hashes); }
			 */
			//List<List<String>> aliases = nested.getListOfAliases(n2a, true);
			// for(List<String>
			// aliases:initialQuery.getListOfAliases(n2a)){
			List<String> firstAliases = new ArrayList<String>();
			for(int i=0;i<nested.getInputTables().size();i++){
				firstAliases.add("nestedalias"+i);
			}
			//nested.renameTables(firstAliases);
			ConjunctiveQueryDecomposer d = new ConjunctiveQueryDecomposer(nested, centralizedExecution,
					addNotNulls);
			Node topSubquery = d.addCQToDAG(nestedNodeOr, hashes);
			// String u=union.dotPrint();
			/*if (addAliases) {
				for (int i = 1; i < aliases.size(); i++) {
					List<String> nextAliases = aliases.get(i);
					topSubquery.addChild(addAliasesToDAG(topSubquery, firstAliases, nextAliases, hashes));
				}
			}*/
			HashCode hc=nestedNode.getHashId();
			if(hashes.containsKey(hc)){
				nestedNode.removeAllChildren();
				nestedNode=hashes.get(hc);
			}
			else{
				hashes.put(hc, nestedNode);
			}
			nested.putNestedNode(nestedNode);
			// nestedNode.removeAllChildren();

		}
		
	}

	// private void computeJoinSimilarities() {
	// try {
	// OverlapAnalyzer.overlapDetection(this.joinLists);
	// } catch (Exception ex) {
	// java.util.logging.Logger.getLogger(QueryDecomposer.class.getName()).log(Level.SEVERE,
	// null, ex);
	// }
	// }

	private void expandDAG(Node eq) {

		for (int i = 0; i < eq.getChildren().size(); i++) {
			// System.out.println(eq.getChildren().size());
			Node op = eq.getChildAt(i);
			if(System.currentTimeMillis()-startTime>getOnlyLeftDeepTreesTime&&!onlyLeft){
				System.out.println("only left!");
				this.onlyLeft=true;
			}
			if (!op.isExpanded()) {
				for (int x = 0; x < op.getChildren().size(); x++) {
					Node inpEq = op.getChildAt(x);
					// System.out.println(eq.getObject());
					// root.dotPrint();
					expandDAG(inpEq);

				}

				// String a=op.getChildAt(0).dotPrint();
				// aplly all possible transfromations to op

				// join commutativity a join b -> b join a
				// This never adds a node because of hashing!!!!!!
				if (op.getObject() instanceof NonUnaryWhereCondition) {

					NonUnaryWhereCondition bwc = (NonUnaryWhereCondition) op.getObject();
					if (bwc.getOperator().equals("=")) {
						Node commutativity = new Node(Node.AND, Node.JOIN);
						NonUnaryWhereCondition newBwc = new NonUnaryWhereCondition();
						newBwc.setOperator("=");
						newBwc.setLeftOp(bwc.getRightOp());
						newBwc.setRightOp(bwc.getLeftOp());
						newBwc.addRangeFilters(bwc);
						commutativity.setObject(newBwc);
						if (op.getChildren().size() > 1) {
							commutativity.addChild(op.getChildAt(1));
						}
						commutativity.addChild(op.getChildAt(0));
						boolean useCommutativity = true;
						if (onlyLeft) {
							for (Node cc : commutativity.getChildren()) {
								Table t = (Table) cc.getObject();
								if (t.getName().startsWith("table")) {
									useCommutativity = false;
									break;
								}
							}
						}
						if (useCommutativity) {
							if (!hashes.containsKey(commutativity.getHashId())) {
								hashes.put(commutativity.getHashId(), commutativity);
								hashes.remove(eq.getHashId());
								for (Node p : eq.getParents()) {
									hashes.remove(p.getHashId());
								}

								eq.addChild(commutativity);

								hashes.put(eq.getHashId(), eq);
								commutativity.addAllDescendantBaseTables(op.getDescendantBaseTables());
								if (useGreedy) {
									for (Integer u : op.getUnions()) {
										commutativity.getUnions().add(u);
									}
								}

								for (Node p : eq.getParents()) {
									hashes.put(p.computeHashID(), p);
								}
							} else {
								unify(eq, hashes.get(commutativity.getHashId()).getFirstParent());
								commutativity.removeAllChildren();

							}
						}
					}
				}

				// join left associativity: a join (b join c) -> (a join b) join
				// c
				// or (a join c) join b
				if (op.getObject() instanceof NonUnaryWhereCondition) {
					NonUnaryWhereCondition bwc = (NonUnaryWhereCondition) op.getObject();
					if (bwc.getOperator().equals("=")) {
						// for (Node c2 : op.getChildren()) {
						if (op.getChildren().size() > 1) {
							Node c2 = op.getChildAt(1);
							for (Node c3 : c2.getChildren()) {
								// if (c2.getChildren().size() > 0) {
								// Node c3 = c2.getChildAt(0);
								if (c3.getObject() instanceof NonUnaryWhereCondition) {
									NonUnaryWhereCondition bwc2 = (NonUnaryWhereCondition) c3.getObject();
									if (bwc2.getOperator().equals("=")) {
										boolean comesFromLeftOp = c3.getChildAt(0).isDescendantOfBaseTable(
												bwc.getRightOp().getAllColumnRefs().get(0).getAlias());
										Node associativity = new Node(Node.AND, Node.JOIN);
										NonUnaryWhereCondition newBwc = new NonUnaryWhereCondition();
										newBwc.setOperator("=");
										/*
										 * if (comesFromLeftOp) {
										 * newBwc.setRightOp(bwc2.getLeftOp());
										 * } else {
										 * newBwc.setRightOp(bwc2.getRightOp());
										 * }
										 */
										newBwc.setRightOp(bwc.getRightOp());
										newBwc.setLeftOp(bwc.getLeftOp());
										newBwc.addRangeFilters(bwc);
										associativity.setObject(newBwc);
										associativity.addChild(op.getChildAt(0));

										if (comesFromLeftOp) {
											associativity.addChild(c3.getChildAt(0));

										} else {
											associativity.addChild(c3.getChildAt(1));

										}
										Node table = new Node(Node.OR);
										table.setObject(new Table("table" + Util.createUniqueId(), null));
										if (hashes.containsKey(associativity.getHashId())) {
											Node assocInHashes = hashes.get(associativity.getHashId());
											table = assocInHashes.getFirstParent();

											if (useGreedy) {
												for (Integer u : eq.getUnions()) {
													assocInHashes.getUnions().add(u);
													table.getUnions().add(u);
												}
											}
											associativity.removeAllChildren();
											// associativity = assocInHashes;

										} else {
											hashes.put(associativity.getHashId(), associativity);
											table.addChild(associativity);

											// table.setPartitionedOn(new
											// PartitionCols(newBwc.getAllColumnRefs()));
											hashes.put(table.getHashId(), table);
											associativity.addAllDescendantBaseTables(
													op.getChildAt(0).getDescendantBaseTables());
											if (useGreedy) {
												for (Integer u : eq.getUnions()) {
													associativity.getUnions().add(u);
													table.getUnions().add(u);
												}
											}

											if (comesFromLeftOp) {
												associativity.addAllDescendantBaseTables(
														c3.getChildAt(0).getDescendantBaseTables());

											} else {
												associativity.addAllDescendantBaseTables(
														c3.getChildAt(1).getDescendantBaseTables());

											}
											table.addAllDescendantBaseTables(associativity.getDescendantBaseTables());
										}

										// table.setPartitionedOn(new
										// PartitionCols(newBwc.getAllColumnRefs()));

										// table.setIsCentralised(c3.getChildAt(0).isCentralised()
										// && op.getChildAt(0).isCentralised());
										Node associativityTop = new Node(Node.AND, Node.JOIN);
										NonUnaryWhereCondition newBwc2 = new NonUnaryWhereCondition();
										newBwc2.setOperator("=");
										if (comesFromLeftOp) {
											newBwc2.setRightOp(bwc2.getRightOp());
											newBwc2.setLeftOp(bwc2.getLeftOp());
										} else {
											newBwc2.setRightOp(bwc2.getLeftOp());
											newBwc2.setLeftOp(bwc2.getRightOp());
										}
										newBwc2.addRangeFilters(bwc2);
										// newBwc2.setLeftOp(bwc.getRightOp());
										associativityTop.setObject(newBwc2);
										associativityTop.addChild(table);

										if (comesFromLeftOp && c3.getChildren().size() > 1) {
											associativityTop.addChild(c3.getChildAt(1));

										} else if (c3.getChildren().size() > 1) {
											associativityTop.addChild(c3.getChildAt(0));

										}
										// System.out.println(associativityTop.getObject().toString());
										if (!hashes.containsKey(associativityTop.getHashId())||hashes.get(associativityTop.getHashId()).getParents().isEmpty()) {
											hashes.put(associativityTop.getHashId(), associativityTop);
											// Node newTop =
											// hashes.checkAndPutWithChildren(associativityTop);
											hashes.remove(eq.getHashId());
											for (Node p : eq.getParents()) {
												hashes.remove(p.getHashId());
											}
											eq.addChild(associativityTop);
											associativityTop.addAllDescendantBaseTables(op.getDescendantBaseTables());
											if (useGreedy) {
												for (Integer u : eq.getUnions()) {
													associativityTop.getUnions().add(u);
												}
											}
											// noOfChildren++;
											// eq.setPartitionedOn(new
											// PartitionCols(newBwc.getAllColumnRefs()));
											// if(!h.containsKey(eq.computeHashID())){
											hashes.put(eq.getHashId(), eq);
											for (Node p : eq.getParents()) {
												hashes.put(p.computeHashID(), p);
											}
											// }
											/*
											 * if
											 * (!h.containsKey(associativityTop
											 * .computeHashID())){
											 * h.putWithChildren
											 * (associativityTop);
											 * h.remove(eq.computeHashID());
											 * eq.addChild(associativityTop);
											 * if(
											 * !h.containsKey(eq.computeHashID
											 * ())){ h.put(eq.computeHashID(),
											 * eq); } else{ //needs unification?
											 * } } else{ //unify
											 * //unify(associativityTop, eq, h);
											 * Node
											 * other=h.get(associativityTop.
											 * computeHashID());
											 * h.remove(eq.computeHashID());
											 * eq.addChild(other);
											 * if(!h.containsKey
											 * (eq.computeHashID())){
											 * h.put(eq.computeHashID(), eq); }
											 * else{ //needs unification? } }
											 */
										} else {

											unify(eq, hashes.get(associativityTop.getHashId()).getFirstParent());
											// same as unify(eq', eq)???
											// checking again children of eq?
											associativityTop.removeAllChildren();
											if (table.getParents().isEmpty()) {
												if (hashes.get(table.getHashId()) == table) {
													hashes.remove(table.getHashId());
												}
												for (Node n : table.getChildren()) {
													if (n.getParents().size() == 1) {
														if (hashes.get(n.getHashId()) == n) {
															hashes.remove(n.getHashId());
														}
													}
												}
												table.removeAllChildren();
											}
											if (associativity.getParents().isEmpty()) {
												if (hashes.get(associativity.getHashId()) == associativity) {
													hashes.remove(associativity.getHashId());
												}
												associativity.removeAllChildren();
											}

											// do we need this?
											/*
											 * Node otherAssocTop =
											 * hashes.get(associativityTop
											 * .getHashId()); if
											 * (!eq.getChildren
											 * ().contains(otherAssocTop)) {
											 * hashes.remove(eq.getHashId());
											 * eq.addChild(otherAssocTop);
											 * noOfChildren++;
											 * //eq.setPartitionedOn(new
											 * PartitionCols
											 * (newBwc.getAllColumnRefs())); //
											 * if
											 * (!h.containsKey(eq.computeHashID
											 * ())){
											 * hashes.put(eq.computeHashID(),
											 * eq); }
											 */
										}
									}
								}
							}
						} else {
							Node c2 = op.getChildAt(0);
							for (int c2Ch = 0; c2Ch < c2.getChildren().size(); c2Ch++) {
								Node c3 = c2.getChildren().get(c2Ch);
								// if (c2.getChildren().size() > 0) {
								// Node c3 = c2.getChildAt(0);
								if (c3.getObject() instanceof NonUnaryWhereCondition && c3.getChildren().size() > 1) {
									NonUnaryWhereCondition bwc2 = (NonUnaryWhereCondition) c3.getObject();
									if (bwc2.getOperator().equals("=")) {
										boolean comesFromLeftOp = c3.getChildAt(0).isDescendantOfBaseTable(
												bwc.getRightOp().getAllColumnRefs().get(0).getAlias());
										boolean comesFromRightOp = c3.getChildAt(0).isDescendantOfBaseTable(
												bwc.getLeftOp().getAllColumnRefs().get(0).getAlias());
										Node associativity = new Node(Node.AND, Node.JOIN);
										if (!comesFromLeftOp && !comesFromRightOp) {
											continue;
										}
										boolean comesFromLeftOp2 = c3.getChildAt(1).isDescendantOfBaseTable(
												bwc.getRightOp().getAllColumnRefs().get(0).getAlias());
										boolean comesFromRightOp2 = c3.getChildAt(1).isDescendantOfBaseTable(
												bwc.getLeftOp().getAllColumnRefs().get(0).getAlias());
										if (!comesFromLeftOp2 && !comesFromRightOp2) {
											continue;
										}
										NonUnaryWhereCondition newBwc = new NonUnaryWhereCondition();
										newBwc.setOperator("=");

										newBwc.setRightOp(bwc.getRightOp());
										newBwc.setLeftOp(bwc.getLeftOp());
										newBwc.addRangeFilters(bwc);
										associativity.setObject(newBwc);

										if (comesFromLeftOp) {
											associativity.addChild(c3.getChildAt(1));
											associativity.addChild(c3.getChildAt(0));

										} else {
											associativity.addChild(c3.getChildAt(0));
											associativity.addChild(c3.getChildAt(1));

										}
										Node table = new Node(Node.OR);
										table.setObject(new Table("table" + Util.createUniqueId(), null));

										if (hashes.containsKey(associativity.getHashId())) {
											Node assocInHashes = hashes.get(associativity.getHashId());
											table = assocInHashes.getFirstParent();
											if (useGreedy) {
												for (Integer u : eq.getUnions()) {
													assocInHashes.getUnions().add(u);
													table.getUnions().add(u);
												}
											}
											associativity.removeAllChildren();
											// associativity = assocInHashes;

										} else {
											hashes.put(associativity.getHashId(), associativity);
											table.addChild(associativity);

											// table.setPartitionedOn(new
											// PartitionCols(newBwc.getAllColumnRefs()));
											hashes.put(table.getHashId(), table);

											if (useGreedy) {
												for (Integer u : eq.getUnions()) {
													associativity.getUnions().add(u);
													c2.getUnions().add(u);
												}
											}

											associativity.addAllDescendantBaseTables(c3.getDescendantBaseTables());

											table.addAllDescendantBaseTables(associativity.getDescendantBaseTables());
										}

										// table.setPartitionedOn(new
										// PartitionCols(newBwc.getAllColumnRefs()));

										// table.setIsCentralised(c3.getChildAt(0).isCentralised()
										// && op.getChildAt(0).isCentralised());
										Node associativityTop = new Node(Node.AND, Node.JOIN);
										NonUnaryWhereCondition newBwc2 = new NonUnaryWhereCondition();
										newBwc2.setOperator("=");
										newBwc2.setRightOp(bwc2.getRightOp());
										newBwc2.setLeftOp(bwc2.getLeftOp());
										// newBwc2.setLeftOp(bwc.getRightOp());
										associativityTop.setObject(newBwc2);
										associativityTop.addChild(table);
										newBwc2.addRangeFilters(bwc2);
										associativityTop.setExpanded(true);

										// System.out.println(associativityTop.getObject().toString());
										if (!hashes.containsKey(associativityTop.getHashId())||hashes.get(associativityTop.getHashId()).getParents().isEmpty()) {
											hashes.put(associativityTop.getHashId(), associativityTop);
											// Node newTop =
											// hashes.checkAndPutWithChildren(associativityTop);
											hashes.remove(eq.getHashId());
											for (Node p : eq.getParents()) {
												hashes.remove(p.getHashId());
											}
											eq.addChild(associativityTop);
											associativityTop.addAllDescendantBaseTables(op.getDescendantBaseTables());
											if (useGreedy) {
												for (Integer u : eq.getUnions()) {
													associativityTop.getUnions().add(u);
												}
											}
											// noOfChildren++;
											// eq.setPartitionedOn(new
											// PartitionCols(newBwc.getAllColumnRefs()));
											// if(!h.containsKey(eq.computeHashID())){
											hashes.put(eq.getHashId(), eq);
											for (Node p : eq.getParents()) {
												hashes.put(p.computeHashID(), p);
											}

										} else {

											unify(eq, hashes.get(associativityTop.getHashId()).getFirstParent());
											// same as unify(eq', eq)???
											// checking again children of eq?
											associativityTop.removeAllChildren();
											if (table.getParents().isEmpty()) {
												if (hashes.get(table.getHashId()) == table) {
													hashes.remove(table.getHashId());
												}
												for (Node n : table.getChildren()) {
													if (n.getParents().size() == 1) {
														if (hashes.get(n.getHashId()) == n) {
															hashes.remove(n.getHashId());
														}
													}
												}
												table.removeAllChildren();
											}
											if (associativity.getParents().isEmpty()) {
												if (hashes.get(associativity.getHashId()) == associativity) {
													hashes.remove(associativity.getHashId());
												}
												associativity.removeAllChildren();
											}

										}
									}
								}
							}

						}
					}

				}

				if (!(op.getObject() instanceof NonUnaryWhereCondition)) {
					op.computeHashID();
				}
				op.setExpanded(true);

			}
		}
		eq.computeHashID();
		if (useSIP && !eq.getParents().isEmpty() && eq.getParents().get(0).getOpCode() == Node.UNION) {
			Projection p = (Projection) eq.getChildAt(0).getObject();
			for (int pChNo = 0; pChNo < eq.getChildAt(0).getChildren().size(); pChNo++) {
				Node joinTable = eq.getChildAt(0).getChildAt(pChNo);
				for (int chNo = 0; chNo < joinTable.getChildren().size(); chNo++) {
					Node join = joinTable.getChildAt(chNo);
					if (join.getChildren().size() == 2) {
						sipInfo.addToSipInfo(p, join, sipToUnions.get(unionnumber));
						// Set<Node> newsip=new HashSet<Node>();
						// newsip.add(join.getChildAt(0));
						// newsip.add(join.getChildAt(1));
						// sipToUnions.get(unionnumber).add(newsip);
					} else if (join.getOpCode() == Node.JOIN) {
						// System.out.println("yes");
						NonUnaryWhereCondition nuwc = (NonUnaryWhereCondition) join.getObject();
						Node joinTable2 = join.getChildAt(0);
						for (int chNo2 = 0; chNo2 < joinTable2.getChildren().size(); chNo2++) {
							Node join2 = joinTable2.getChildAt(chNo2);
							if (join2.getChildren().size() == 2) {
								sipInfo.addToSipInfo(p, join2, sipToUnions.get(unionnumber), nuwc);
								// Set<Node> newsip=new HashSet<Node>();
								// newsip.add(join.getChildAt(0));
								// newsip.add(join.getChildAt(1));
								// sipToUnions.get(unionnumber).add(newsip);
							}
						}
					}
				}
			}
			unionnumber++;
			sipToUnions.put(unionnumber, new HashSet<SipNode>());
		}
	}

	private void unify(Node q, Node q2) {
		if (q == q2) {
			return;
		}

		hashes.remove(q2.getHashId());
		hashes.remove(q.getHashId());
		for (Node p : q.getParents()) {
			hashes.remove(p.getHashId());
			p.setHashNeedsRecomputing();
		}
		q.getUnions().addAll(q2.getUnions());

		for (Node c : q2.getChildren()) {
			q.addChild(c);
			q.addAllDescendantBaseTables(c.getDescendantBaseTables());
		}
		for (Node p : q.getParents()) {
			hashes.put(p.getHashId(), p);
		}
		q2.removeAllChildren();
		for (int i = 0; i < q2.getParents().size(); i++) {
			Node p = q2.getParents().get(i);
			// System.out.println(p.getHashId());
			hashes.remove(p.getHashId());
			int pos = p.removeChild(q2);
			i--;
			if (p.getParents().isEmpty()) {
				continue;
			}
			p.addChildAt(q, pos);
			if (hashes.containsKey(p.getHashId())&&!hashes.get(p.getHashId()).getParents().isEmpty()) {
				// System.out.println("further unification!");
				unify(hashes.get(p.getHashId()).getFirstParent(), p.getFirstParent());
			} else {
				hashes.put(p.getHashId(), p);
			}
			// System.out.println(p.getHashId());
		}
		hashes.put(q.getHashId(), q);
	}

	private Iterator<PartitionCols> combineColumns(Set<PartitionCols> partitionedOn) {
		Iterator<PartitionCols> it = partitionedOn.iterator();
		List<PartitionCols> resultCols = new ArrayList<PartitionCols>();
		resultCols.add(it.next());
		while (it.hasNext()) {
			PartitionCols pc = it.next();
			// Iterator<PartitionCols> it2 = resultCols.iterator();
			for (int i = 0; i < resultCols.size(); i++) {
				PartitionCols pc2 = resultCols.get(i);
				// }){
				// while (it2.hasNext()) {
				// PartitionCols pc2 = it2.next();
				boolean toAdd = true;
				for (Column c : pc.getColumns()) {
					for (Column c2 : pc2.getColumns()) {
						if (c2.equals(c)) {
							pc2.addColumns(pc.getColumns());
							toAdd = false;
							break;
						}
					}
				}
				if (toAdd && !resultCols.contains(pc)) {
					resultCols.add(pc);
				}
			}
		}
		return resultCols.iterator();
	}

	private Node addAliasesToDAG(Node parent, List<String> firstAliases, List<String> nextAliases, NodeHashValues h) {
		// for(int i=0;i<parent.getChildren().size();i++){
		Node opNode = parent.getChildAt(0);

		List<Node> newChidlren = new ArrayList<Node>();
		for (Node inpEq : opNode.getChildren()) {
			Table t = (Table) inpEq.getObject();
			if (!t.getName().startsWith("table")) {
				Node newBaseTable = new Node(Node.OR);
				Table t2 = new Table(t.getName(), nextAliases.get(firstAliases.indexOf(t.getAlias())));
				newBaseTable.setObject(t2);
				if (!h.containsKey(newBaseTable.getHashId())) {
					h.put(newBaseTable.getHashId(), newBaseTable);
					newBaseTable.addDescendantBaseTable(t2.getAlias());
				}
				newChidlren.add(h.get(newBaseTable.getHashId()));
			} else {
				Node newEqNode = new Node(Node.OR);
				newEqNode.setObject(new Table("table" + Util.createUniqueId(), null));
				newEqNode.addChild(addAliasesToDAG(inpEq, firstAliases, nextAliases, h));
				if (!h.containsKey(newEqNode.getHashId())) {
					h.put(newEqNode.getHashId(), newEqNode);
					for (Node n : newEqNode.getChildren()) {
						newEqNode.addAllDescendantBaseTables(n.getDescendantBaseTables());
					}
				} else {
					newEqNode = h.get(newEqNode.getHashId());
					// System.out.println("what?");
				}
				newChidlren.add(newEqNode);
			}
		}

		Operand op = (Operand) opNode.getObject();
		Node newOpNode = new Node(Node.AND, opNode.getOpCode());
		for (Node c : newChidlren) {
			newOpNode.addChild(c);
			newOpNode.addAllDescendantBaseTables(c.getDescendantBaseTables());
		}
		// newOpNode.addChild(newEqNode);
		Operand cloned = null;
		try {
			cloned = op.clone();
		} catch (CloneNotSupportedException ex) {
			java.util.logging.Logger.getLogger(QueryDecomposer.class.getName()).log(Level.SEVERE, null, ex);
		}
		newOpNode.setObject(cloned);
		for (Column c : cloned.getAllColumnRefs()) {
			for (int j = 0; j < firstAliases.size(); j++) {
				if (c.getAlias().equals(firstAliases.get(j))) {
					c.setAlias(nextAliases.get(j));
					break;
				}
			}
		}
		if (h.containsKey(newOpNode.getHashId())) {
			return h.get(newOpNode.getHashId());
		} else {
			h.put(newOpNode.getHashId(), newOpNode);

			return newOpNode;
		}
		// }
	}

	int total = 0;
	int pruned = 0;

	private SinglePlan getBestPlan(Node e, Column c, double limit, double repCost,
			EquivalentColumnClasses partitionRecord, Set<MemoKey> toMaterialize, Memo memo) {
		MemoKey ec = new MemoKey(e, c);
		SinglePlan resultPlan;
		if (memo.containsMemoKey(ec) && memo.getMemoValue(ec).isMaterialised()) {
			// check on c!
			resultPlan = new SinglePlan(0.0, null);
			PartitionedMemoValue pmv = (PartitionedMemoValue) memo.getMemoValue(ec);
			partitionRecord.setLastPartitioned(pmv.getDlvdPart());
		} else if (memo.containsMemoKey(ec)) {
			resultPlan = memo.getMemoValue(ec).getPlan();
			PartitionedMemoValue pmv = (PartitionedMemoValue) memo.getMemoValue(ec);
			partitionRecord.setLastPartitioned(pmv.getDlvdPart());
		} else {
			resultPlan = searchForBestPlan(e, c, limit, repCost, partitionRecord, toMaterialize, memo);
		}
		if (resultPlan != null && resultPlan.getCost() < limit) {
			return resultPlan;
		} else {
			return null;
		}
	}

	private SinglePlan searchForBestPlan(Node e, Column c, double limit, double repCost,
			EquivalentColumnClasses partitionRecord, Set<MemoKey> toMaterialize, Memo memo) {

		if (!e.getObject().toString().startsWith("table")) {
			// base table
			SinglePlan r = new SinglePlan(0);
			memo.put(e, r, c, repCost, true, null);
			return r;
		}

		SinglePlan resultPlan = new SinglePlan(Integer.MAX_VALUE);
		double repartitionCost = 0;
		if (c != null) {
			repartitionCost = nce.estimateRepartition(e, c);
		}
		/*
		 * PartitionCols e2partCols = new PartitionCols(); Node np = new
		 * Node(Node.OR); np.setPartitionedOn(e2partCols);
		 * e2partCols.addColumn(c); //
		 * if(e.getObject().toString().startsWith("table")){ // np.setObject(new
		 * Table("table" + Util.createUniqueId(), null));} // else{
		 * np.setObject(e.getObject()); // }
		 */
		// memo.put(ec, np);
		// e2Plan;
		// for (Node o : e.getChildren()) {
		for (int k = 0; k < e.getChildren().size(); k++) {
			EquivalentColumnClasses e2RecordCloned = partitionRecord.shallowCopy();
			Node o = e.getChildAt(k);
			SinglePlan e2Plan = new SinglePlan(Integer.MAX_VALUE);
			Double opCost = nce.getCostForOperator(o);
			if (o.getOpCode() == Node.JOIN) {
				NonUnaryWhereCondition join = (NonUnaryWhereCondition) o.getObject();
				e2RecordCloned.mergePartitionRecords(join);
			}

			List<MemoKey> toMatE2 = new ArrayList<MemoKey>();
			// this must go after algorithmic implementation
			limit -= opCost;
			double algLimit;
			EquivalentColumnClasses algRecordCloned;
			int cComesFromChildNo = -1;
			for (int m = 0; m < o.getAlgorithmicImplementations().length; m++) {

				int a = o.getAlgorithmicImplementations()[m];
				int retainsPartition = -1;
				retainsPartition = getRetainsPartition(a);
				PartitionCols returnedPt = null;
				algRecordCloned = e2RecordCloned.shallowCopy();
				algLimit = limit;
				Set<MemoKey> toMatAlg = new HashSet<MemoKey>();

				SinglePlan algPlan = new SinglePlan(opCost);
				algPlan.setChoice(k);
				if (a == Node.NESTED) {
					// nested is always materialized
					toMatAlg.add(new MemoKey(e, c));
					// algPlan.increaseCost(cost mat e)
				}
				if (c != null && guaranteesResultPtnedOn(a, o, c)) {
					algRecordCloned.setLastPartitioned(algRecordCloned.getClassForColumn(c));
				}
				for (int i = 0; i < o.getChildren().size(); i++) {
					EquivalentColumnClasses oRecord = algRecordCloned.shallowCopy();
					Node e2 = o.getChildAt(i);
					if (m == 0 && c != null && cComesFromChildNo < 0) {
						if (e2.isDescendantOfBaseTable(c.getAlias())) {
							cComesFromChildNo = i;
						}
					}

					// double minRepCost = repCost < repartitionCost ?
					// repCost:repCost;
					Column c2 = getPartitionRequired(a, o, i);
					Double c2RepCost = 0.0;
					if (c2 != null) {
						nce.estimateRepartition(e2, c2);
					}

					if (c == null || cComesFromChildNo != i || guaranteesResultPtnedOn(a, o, c)) {
						// algPlan.append(getBestPlan(e2, c2, memo, algLimit,
						SinglePlan t = getBestPlan(e2, c2, algLimit, c2RepCost, oRecord, toMatAlg, memo);
						algPlan.addInputPlan(e2, c2);
						algPlan.increaseCost(t.getCost());
					} else if (guaranteesResultNotPtnedOn(a, o, c)) {

						if (repartitionCost < repCost) {
							algPlan.addRepartitionBeforeOp(c);
							// algPlan.getPath().addOption(-1);
							oRecord.setClassRepartitioned(c, true);
							toMatAlg.add(new MemoKey(e, c));
							algLimit -= repartitionCost;
						} else {
							oRecord.setClassRepartitioned(c, false);
						}
						// algPlan.append(getBestPlan(e2, c2, memo, algLimit,
						// c2RepCost, cel, partitionRecord, toMatAlg));
						SinglePlan t = getBestPlan(e2, c2, algLimit, c2RepCost, oRecord, toMatAlg, memo);
						algPlan.addInputPlan(e2, c2);
						algPlan.increaseCost(t.getCost());
					} else {
						// algPlan.append(getBestPlan(e2, c, memo, algLimit,
						// c2RepCost, cel, partitionRecord, toMatAlg));
						SinglePlan t = getBestPlan(e2, c, algLimit, c2RepCost, oRecord, toMatAlg, memo);
						algPlan.addInputPlan(e2, c);
						algPlan.increaseCost(t.getCost());
						if (oRecord.getLast() == null
								|| (!oRecord.getLast().contains(c) && repartitionCost < repCost)) {
							algPlan.addRepartitionBeforeOp(c);
							// algPlan.getPath().addOption(-1);
							oRecord.setClassRepartitioned(c, true);
							toMatAlg.add(new MemoKey(e, c));
							algLimit -= repartitionCost;
						}
						algLimit -= algPlan.getCost();
					}
					if (c2 != null) {
						if (oRecord.getLast() == null || !oRecord.getLast().contains(c2)) {
							// algPlan.getPath().addOption(-1);
							memo.getMemoValue(new MemoKey(e2, c2)).getPlan().addRepartitionBeforeOp(c2);
							// algPlan.addRepartitionAfterOp(i, c2);
							if (algPlan.getRepartitionBeforeOp() != null) {
								oRecord.setClassRepartitioned(c2, false);
							} else {
								oRecord.setClassRepartitioned(c2, true);
							}
							toMatAlg.add(new MemoKey(e2, c2));
							algLimit -= c2RepCost;
						}
					}
					double e2PlanCost = algPlan.getCost();
					if (c != null) {
						if (oRecord.getLast() == null || !oRecord.getLast().contains(c)) {
							e2PlanCost += repartitionCost;
						}
					}
					algLimit -= e2PlanCost;
					// algRecordCloned.addClassesFrom(oRecord);
					if (i == retainsPartition) {
						returnedPt = oRecord.getLast();
					}
				}
				if (returnedPt != null) {
					algRecordCloned.setLastPartitioned(returnedPt);
				}
				if (algPlan.getCost() < e2Plan.getCost()) {
					e2Plan = algPlan;
					toMatE2.addAll(toMatAlg);
					e2RecordCloned = algRecordCloned;
				}
			}
			if (e2Plan.getCost() < resultPlan.getCost()) {
				resultPlan = e2Plan;
				toMaterialize.addAll(toMatE2);
				partitionRecord.copyFrom(e2RecordCloned);
				memo.put(e, resultPlan, c, repCost, e2RecordCloned.getLast(), toMaterialize);
			}
		}
		if (!e.getParents().isEmpty() && (e.getParents().get(0).getOpCode() == Node.UNION
				|| e.getParents().get(0).getOpCode() == Node.UNIONALL)) {
			// String g = e.dotPrint();
			for (MemoKey mv : toMaterialize) {
				memo.getMemoValue(mv).setMaterialized(true);
			}
			toMaterialize.clear();
			// e.setPlanMaterialized(resultPlan.getPath().getPlanIterator());
		}
		return resultPlan;

	}

	private SinglePlan getBestPlanPruned(Node e, Column c, double limit, double repCost,
			EquivalentColumnClasses partitionRecord, Set<MemoKey> toMaterialize, Memo memo) {
		if (limits.containsKey(e)) {
			if (limits.get(e) > limit - repCost) {
				return null;
			}
		}
		MemoKey ec = new MemoKey(e, c);
		SinglePlan resultPlan;
		if (memo.containsMemoKey(ec) && memo.getMemoValue(ec).isMaterialised()) {
			// check on c!
			resultPlan = new SinglePlan(0.0, null);
			PartitionedMemoValue pmv = (PartitionedMemoValue) memo.getMemoValue(ec);
			partitionRecord.setLastPartitioned(pmv.getDlvdPart());
		} else if (memo.containsMemoKey(ec)) {
			resultPlan = memo.getMemoValue(ec).getPlan();
			PartitionedMemoValue pmv = (PartitionedMemoValue) memo.getMemoValue(ec);
			toMaterialize.addAll(pmv.getToMat());
			partitionRecord.setLastPartitioned(pmv.getDlvdPart());
			// if(pmv.isUsed()){
			// pmv.setMaterialized(true);
			// }
		} else {
			resultPlan = searchForBestPlanPruned(e, c, limit, repCost, partitionRecord, toMaterialize, memo);
		}
		// if (resultPlan != null && resultPlan.getCost() < limit) {
		return resultPlan;
		// } else {
		// return null;
		// }
	}

	private SinglePlan searchForBestPlanPruned(Node e, Column c, double limit, double repCost,
			EquivalentColumnClasses partitionRecord, Set<MemoKey> toMaterialize, Memo memo) {

		if (useCache && registry.containsKey(e.getHashId()) && e.getHashId() != null) {
			madgik.exareme.common.schema.Table t = registry.get(e.getHashId());
			String col = Registry.getInstance(db).getPartitionColumn(t.getName());
			int ptns = Registry.getInstance(db).getNumOfPartitions(t.getName());
			
			if (c.getName().equals(col)&&ptns==noOfparts) {
				SinglePlan r = new SinglePlan(0);

				memo.put(e, r, true, true, false);

				return r;
			}
		}

		if (!e.getObject().toString().startsWith("table")) {
			// base table
			SinglePlan r = new SinglePlan(0);
			memo.put(e, r, c, repCost, false, null);
			PartitionedMemoValue pmv = (PartitionedMemoValue) memo.getMemoValue(new MemoKey(e ,c));
			pmv.setToMat(toMaterialize);
			//toMaterialize.add(new MemoKey(e, c));
			partitionRecord.setLastPartitioned(null);
			return r;
		}

		SinglePlan resultPlan = null;
		double repartitionCost = 0;
		if (c != null) {
			repartitionCost = nce.estimateRepartition(e, c);
		}

		/*
		 * PartitionCols e2partCols = new PartitionCols(); Node np = new
		 * Node(Node.OR); np.setPartitionedOn(e2partCols);
		 * e2partCols.addColumn(c); //
		 * if(e.getObject().toString().startsWith("table")){ // np.setObject(new
		 * Table("table" + Util.createUniqueId(), null));} // else{
		 * np.setObject(e.getObject()); // }
		 */

		// memo.put(ec, np);
		// e2Plan;
		// for (Node o : e.getChildren()) {
		for (int k = 0; k < e.getChildren().size(); k++) {
			EquivalentColumnClasses e2RecordCloned = partitionRecord.shallowCopy();
			Node o = e.getChildAt(k);

			Double opCost = nce.getCostForOperator(o);
			SinglePlan e2Plan = null;
			// this must go after algorithmic implementation
			double newLimit = limit - opCost;
			if (newLimit < 0) {
				continue;
			}
			if (o.getOpCode() == Node.JOIN) {
				NonUnaryWhereCondition join = (NonUnaryWhereCondition) o.getObject();
				e2RecordCloned.mergePartitionRecords(join);
			}
			List<MemoKey> toMatE2 = new ArrayList<MemoKey>();

			double algLimit;
			EquivalentColumnClasses algRecordCloned;
			int cComesFromChildNo = -1;
			for (int m = 0; m < o.getAlgorithmicImplementations().length; m++) {

				int a = o.getAlgorithmicImplementations()[m];
				
				int retainsPartition = -1;
				retainsPartition = getRetainsPartition(a);
				PartitionCols returnedPt = null;
				algRecordCloned = e2RecordCloned.shallowCopy();
				algLimit = newLimit;
				Set<MemoKey> toMatAlg = new HashSet<MemoKey>();
				
				SinglePlan algPlan = new SinglePlan(opCost);
				algPlan.setChoice(k);
				if (c != null && guaranteesResultPtnedOn(a, o, c)) {
					algRecordCloned.setLastPartitioned(algRecordCloned.getClassForColumn(c));
				}
				for (int i = 0; i < o.getChildren().size(); i++) {
					EquivalentColumnClasses oRecord = algRecordCloned.shallowCopy();
					Node e2 = o.getChildAt(i);
					if (m == 0 && c != null && cComesFromChildNo < 0) {
						if (e2.isDescendantOfBaseTable(c.getAlias())) {
							cComesFromChildNo = i;
						}
					}

					// double minRepCost = repCost < repartitionCost ?
					// repCost:repCost;
					Column c2 = getPartitionRequired(a, o, i);
					Double c2RepCost = 0.0;
					if (c2 != null) {
						c2RepCost = nce.estimateRepartition(e2, c2);
					}
					
					if(a==Node.LEFTJOIN){
						//TODO optimize this
						//for now set children materialized
						toMatAlg.add(new MemoKey(e2, c2));
					}

					if (c == null || cComesFromChildNo != i || guaranteesResultPtnedOn(a, o, c)) {
						// algPlan.append(getBestPlan(e2, c2, memo, algLimit,
						// c2RepCost, cel, partitionRecord, toMatAlg));
						SinglePlan t = getBestPlanPruned(e2, c2, algLimit, c2RepCost, oRecord, toMatAlg, memo);
						if (t == null) {
							continue;
						}
						algPlan.addInputPlan(e2, c2);
						algPlan.increaseCost(t.getCost());
						algLimit -= t.getCost();
					} else if (guaranteesResultNotPtnedOn(a, o, c)) {

						if (repartitionCost < repCost) {
							algPlan.addRepartitionBeforeOp(c);
							algLimit -= repartitionCost;

							if (algLimit < 0) {
								continue;
							}
							algPlan.increaseCost(repartitionCost);
							// algPlan.getPath().addOption(-1);
							oRecord.setClassRepartitioned(c, true);
							toMatAlg.add(new MemoKey(e, c));

						} else {
							algLimit -= repCost;
							if (algLimit < 0) {
								continue;
							}
							algPlan.increaseCost(repCost);
							oRecord.setClassRepartitioned(c, false);
						}
						// algPlan.append(getBestPlan(e2, c2, memo, algLimit,
						// c2RepCost, cel, partitionRecord, toMatAlg));
						SinglePlan t = getBestPlanPruned(e2, c2, algLimit, c2RepCost, oRecord, toMatAlg, memo);
						if (t == null) {
							continue;
						}
						algPlan.addInputPlan(e2, c2);
						algPlan.increaseCost(t.getCost());
						algLimit -= t.getCost();
					} else {
						// algPlan.append(getBestPlan(e2, c, memo, algLimit,
						// c2RepCost, cel, partitionRecord, toMatAlg));
						SinglePlan t = getBestPlanPruned(e2, c, algLimit, c2RepCost, oRecord, toMatAlg, memo);
						if (t == null) {
							continue;
						}
						algPlan.addInputPlan(e2, c);
						algPlan.increaseCost(t.getCost());
						algLimit -= t.getCost();
						if (oRecord.getLast() == null
								|| (!oRecord.getLast().contains(c) && repartitionCost < repCost)) {

							// here do not add repCost. it has been added before
							algPlan.addRepartitionBeforeOp(c);
							// algPlan.getPath().addOption(-1);
							oRecord.setClassRepartitioned(c, true);
							toMatAlg.add(new MemoKey(e, c));
							// algLimit -= repartitionCost;
						}
						// algLimit -= algPlan.getCost();
					}
					if (c2 != null) {
						if (oRecord.getLast() == null || !oRecord.getLast().contains(c2)) {
							// algPlan.getPath().addOption(-1);
							// algPlan.addRepartitionAfterOp(i, c2);
							memo.getMemoValue(new MemoKey(e2, c2)).getPlan().addRepartitionBeforeOp(c2);
							if (algPlan.getRepartitionBeforeOp() != null) {
								oRecord.setClassRepartitioned(c2, false);
							} else {
								oRecord.setClassRepartitioned(c2, true);
							}
							toMatAlg.add(new MemoKey(e2, c2));
							algLimit -= c2RepCost;
							if (algLimit < 0) {
								continue;
							}
							algPlan.increaseCost(c2RepCost);
						}
					}
					

					// mark as materialised the result of federated execution
					 if(a==Node.BASEPROJECT &&
					 ((Table)e2.getObject()).isFederated()){
					toMatAlg.add(new MemoKey(e, c));
					 }
					 
					 

					// double e2PlanCost = algPlan.getCost();
					if (c != null) {
						if (oRecord.getLast() == null || !oRecord.getLast().contains(c)) {
							algLimit -= repartitionCost;
							if (algLimit < 0) {
								continue;
							}
							// e2PlanCost += repartitionCost;
						}
					}
					// algLimit -= e2PlanCost;
					// algRecordCloned.addClassesFrom(oRecord);
					if (i == retainsPartition) {
						returnedPt = oRecord.getLast();
					}
				}
				if (returnedPt != null) {
					algRecordCloned.setLastPartitioned(returnedPt);
				}
				if (e2Plan == null || algPlan.getCost() < e2Plan.getCost()) {
					e2Plan = algPlan;
					// toMatE2.clear();
					toMatE2.addAll(toMatAlg);
					e2RecordCloned = algRecordCloned;
				}
				if (a == Node.NESTED) {
					//children must not be materialized
					toMatE2.clear();
					// nested is always materialized
					toMatE2.add(new MemoKey(e, c));
					
				}
			}
			if (resultPlan == null || e2Plan.getCost() < resultPlan.getCost()) {
				resultPlan = e2Plan;
				// toMaterialize.clear();
				toMaterialize.addAll(toMatE2);
				partitionRecord.copyFrom(e2RecordCloned);
				memo.put(e, resultPlan, c, repCost, e2RecordCloned.getLast(), toMaterialize);
				if (!e.getParents().isEmpty() && (e.getParents().get(0).getOpCode() == Node.UNION
						|| e.getParents().get(0).getOpCode() == Node.UNIONALL)) {

					limit = resultPlan.getCost();
					System.out.println("prune: " + e.getObject() + "with limit:" + limit);
				}
			}
		}
		if (!e.getParents().isEmpty() && (e.getParents().get(0).getOpCode() == Node.UNION
				|| e.getParents().get(0).getOpCode() == Node.UNIONALL)) {
			// what about other union (alias?)
			// String g = e.dotPrint();
			for (MemoKey mv : toMaterialize) {
				memo.getMemoValue(mv).setMaterialized(true);
			}
			// memo.getMemoValue(new MemoKey(e, c)).setMaterialized(true);
			toMaterialize.clear();
			//
			// e.setPlanMaterialized(resultPlan.getPath().getPlanIterator());
		}
		if (resultPlan == null) {
			System.out.println("pruned!!!");
			limits.put(e, limit - repCost);
		}
		if (!e.getParents().isEmpty() && (e.getParents().get(0).getOpCode() == Node.UNION
				|| e.getParents().get(0).getOpCode() == Node.UNIONALL)) {
			// String g = e.dotPrint();
			memo.setPlanUsed(new MemoKey(e, c));
			// e.setPlanMaterialized(resultPlan.getPath().getPlanIterator());
		}
		return resultPlan;

	}

	private SinglePlan getBestPlanCentralized(Node e, double limit, Memo memo, Map<Node, Double> greedyToMat) {
		MemoKey ec = new MemoKey(e, null);
		SinglePlan resultPlan;
		if (memo.containsMemoKey(ec) && memo.getMemoValue(ec).isMaterialised()) {
			// check on c!
			resultPlan = new SinglePlan(nce.getReadCost(e), null);
		} else if (memo.containsMemoKey(ec)) {
			CentralizedMemoValue cmv = (CentralizedMemoValue) memo.getMemoValue(ec);
			if (!useGreedy) {
				int used = cmv.getUsed();
				// if(cmv.isInvalidated()){
				resultPlan = searchForBestPlanCentralized(e, limit, memo, greedyToMat);
				cmv = (CentralizedMemoValue) memo.getMemoValue(ec);
				cmv.addUsed(used);
				// }

				if (nce.isProfitableToMat(e, cmv.getUsed() + 1, resultPlan.getCost())) {
					memo.removeUsageFromChildren(ec, cmv.getUsed(), unionnumber);
					cmv.setMaterialized(true);
					cmv.setMatUnion(unionnumber);

					resultPlan = new SinglePlan(0.0, null);
					// cmv.setPlan(resultPlan);
				}
			} else {
				resultPlan = memo.getMemoValue(ec).getPlan();
				if (greedyToMat.containsKey(e)) {
					cmv.setMaterialized(true);
					e.setMaterialised(true);
					// greedyToMat.put(e, resultPlan.getCost() +
					// NodeCostEstimator.getWriteCost(e) * 0.0);
					resultPlan.setCost(nce.getReadCost(e));
				}
			}
		} else {
			resultPlan = searchForBestPlanCentralized(e, limit, memo, greedyToMat);
			CentralizedMemoValue cmv = (CentralizedMemoValue) memo.getMemoValue(ec);
			if (greedyToMat.containsKey(e) && e.getFirstParent().getOpCode() != Node.UNION) {
				cmv.setMaterialized(true);
				e.setMaterialised(true);
				greedyToMat.put(e, resultPlan.getCost() * workers + nce.getWriteCost(e) * 2);
				//TODO consider parallelism
				//for now * # of workers
				resultPlan.setCost(nce.getReadCost(e));
			}
		}
		if (resultPlan != null && resultPlan.getCost() < limit) {
			return resultPlan;
		} else {
			return null;
		}
	}

	private SinglePlan searchForBestPlanCentralized(Node e, double limit, Memo memo, Map<Node, Double> greedyToMat) {

		if (useCache && registry.containsKey(e.getHashId()) && e.getHashId() != null) {
			SinglePlan r = new SinglePlan(0);

			memo.put(e, r, true, true, false);

			return r;
		}
		if (!e.getObject().toString().startsWith("table")) {
			// base table
			Table t = (Table) e.getObject();
			SinglePlan r = new SinglePlan(0);

			memo.put(e, r, true, true, t.isFederated());

			return r;
		}

		SinglePlan resultPlan = new SinglePlan(Double.MAX_VALUE);;

		for (int k = 0; k < e.getChildren().size(); k++) {
			Node o = e.getChildAt(k);
			SinglePlan e2Plan = new SinglePlan(Double.MAX_VALUE);
			Double opCost = nce.getCostForOperator(o);
			boolean fed = false;
			boolean mat = false;
			// this must go after algorithmic implementation
			limit -= opCost;
			//if (limit < 0) {
			//	continue;
			//}
			double algLimit;
			// int cComesFromChildNo = -1;
			// for (int m = 0; m < o.getAlgorithmicImplementations().length;
			// m++) {

			// int a = o.getAlgorithmicImplementations()[m];
			algLimit = limit;

			SinglePlan algPlan = new SinglePlan(opCost);
			algPlan.setChoice(k);

			for (int i = 0; i < o.getChildren().size(); i++) {
				Node e2 = o.getChildAt(i);
				
				// algPlan.append(getBestPlan(e2, c2, memo, algLimit, c2RepCost,
				// cel, partitionRecord, toMatAlg));
				SinglePlan t = getBestPlanCentralized(e2, algLimit, memo, greedyToMat);
				algPlan.addInputPlan(e2, null);
				algPlan.increaseCost(t.getCost());

				CentralizedMemoValue cmv = (CentralizedMemoValue) memo.getMemoValue(new MemoKey(e2, null));
				if (o.getOpCode() == Node.NESTED) {
					mat = true;
					Util.setDescNotMaterialised(e2, memo);
				}
				if (o.getOpCode() == Node.LEFTJOIN||o.getOpCode()==Node.JOINKEY){
					cmv.setMaterialized(true);
				}
				if (cmv.isFederated()) {
					if (o.getOpCode() == Node.JOIN) {
						cmv.setMaterialized(true);
					} else {
						fed = true;

						/*
						 * if(o.getOpCode() == Node.PROJECT || o.getOpCode() ==
						 * Node.SELECT){ //check to make materialise base
						 * projections
						 * if(!o.getChildAt(0).getChildren().isEmpty()){ Node
						 * baseProjection=o.getChildAt(0).getChildAt(0);
						 * if(baseProjection.getOpCode()==Node.PROJECT &&
						 * !baseProjection.getChildAt(0).getObject().toString().
						 * startsWith("table")){ //base projection indeed
						 * CentralizedMemoValue cmv2 = (CentralizedMemoValue)
						 * memo.getMemoValue(new MemoKey(o.getChildAt(0),
						 * null)); cmv2.setMaterialized(true); fed = false; } }
						 * }
						 */
					}
				}

				algLimit -= algPlan.getCost();
				//if(algLimit<0){
				//	continue;
				//}

			}

			if ( algPlan.getCost() < e2Plan.getCost()) {
				e2Plan = algPlan;
			}

			
			// }
			if (e2Plan.getCost() < resultPlan.getCost()) {
				resultPlan = e2Plan;
				memo.put(e, resultPlan, mat, false, fed);
				/*if (!e.getParents().isEmpty() && (e.getParents().get(0).getOpCode() == Node.UNION
						|| e.getParents().get(0).getOpCode() == Node.UNIONALL)) {

					limit = resultPlan.getCost();
					System.out.println("prune: " + e.getObject() + "with limit:" + limit);
				}*/

			}

			/*
			 * if(useSIP&&!e.getParents().isEmpty()&&e.getParents().get(0).
			 * getOpCode()==Node.PROJECT){ Projection
			 * p=(Projection)e.getParents().get(0).getObject();
			 * CentralizedMemoValue
			 * cmv=(CentralizedMemoValue)memo.getMemoValue(new MemoKey(e,
			 * null)); Node join=e.getChildAt(cmv.getPlan().getChoice());
			 * if(join.getChildren().size()==2 ){ sipInfo.markSipUsed(p, join);
			 * } }
			 */
		}

		if (!e.getParents().isEmpty() && (e.getParents().get(0).getOpCode() == Node.UNION
				|| e.getParents().get(0).getOpCode() == Node.UNIONALL)) {
			// String g = e.dotPrint();
			if (this.useSIP) {
				getUsedSips(e.getChildAt(memo.getMemoValue(new MemoKey(e, null)).getPlan().getChoice()), memo,
						unionnumber);
			}

			unionnumber++;
			// e.setMaterialised(true);
			// e.setPlanMaterialized(resultPlan.getPath().getPlanIterator());
		}
		return resultPlan;

	}

	private void getUsedSips(Node op, Memo memo, int uNo) {
		if (op.getOpCode() == Node.JOIN && op.getChildren().size() == 2) {
			Set<SipNode> usips = sipToUnions.get(uNo);
			for (SipNode us : usips) {
				if (us.getNode().equals(op.getChildAt(0))) {
					if (this.sipInfo.getSipInfo(us.getSipInfo()) != null) {
						// if(this.sipInfo.getSipInfo(us.getSipInfo()).contains(op.getChildAt(0))){

						us.getSipInfo().increaseCounter();
						// }
					}
				} else if (us.getSipInfo().getJoinNode().equals(op.getChildAt(1).getObject().toString())) {
					if (this.sipInfo.getSipInfo(us.getSipInfo()) != null) {
						// if(this.sipInfo.getSipInfo(us.getSipInfo()).contains(op.getChildAt(0))){

						us.getSipInfo().increaseCounter();
					}
				}
			}

		}

		for (Node c : op.getChildren()) {
			if (c.isMaterialised()) {
				continue;
			}
			if (!c.getChildren().isEmpty()) {
				getUsedSips(c.getChildAt(memo.getMemoValue(new MemoKey(c, null)).getPlan().getChoice()), memo, uNo);
			}
		}

	}

	private Column getPartitionRequired(int a, Node o, int i) {
		if (o.getOpCode() == Node.JOIN) {
			NonUnaryWhereCondition join = (NonUnaryWhereCondition) o.getObject();
			if (o.getChildren().size() == 1) {
				// filter join
				return null;
			}
			if (a == Node.REPARTITIONJOIN) {
				return join.getOp(i).getAllColumnRefs().get(0);
			} else if (a == Node.LEFTBROADCASTJOIN) {
				if (i == 0) {
					return join.getLeftOp().getAllColumnRefs().get(0);
				} else {
					return null;
				}
			} else {
				if (i == 1) {
					return join.getRightOp().getAllColumnRefs().get(0);
				} else {
					return null;
				}
			}
		} else if (o.getOpCode() == Node.LEFTJOIN ||o.getOpCode() == Node.JOINKEY) {
			if (o.getChildren().size() == 1) {
				// filter join
				return null;
			}
			Operand op=(Operand)o.getObject();
			return QueryUtils.getJoinColumnFromOperand(o, op, i);
		} else {
			return null;
		}
	}

	private boolean guaranteesResultPtnedOn(int a, Node o, Column c) {
		if (o.getOpCode() == Node.JOIN) {

			if (a == Node.REPARTITIONJOIN) {
				NonUnaryWhereCondition join = (NonUnaryWhereCondition) o.getObject();
				return c.equals(join.getOp(0).getAllColumnRefs().get(0))
						|| c.equals(join.getOp(1).getAllColumnRefs().get(0));
			} else {
				return false;
			}

			/*
			 * else if(a==Node.LEFTBROADCASTJOIN){ return
			 * c.equals(join.getOp(0).getAllColumnRefs().get(0)); } else{ return
			 * c.equals(join.getOp(1).getAllColumnRefs().get(0)); }
			 */
		} else {
			return false;
		}
	}

	private boolean guaranteesResultNotPtnedOn(int a, Node o, Column c) {
		if (o.getOpCode() == Node.JOIN) {

			if (a == Node.REPARTITIONJOIN) {
				NonUnaryWhereCondition join = (NonUnaryWhereCondition) o.getObject();
				return !(c.equals(join.getOp(0).getAllColumnRefs().get(0))
						|| c.equals(join.getOp(1).getAllColumnRefs().get(0)));
			} else {
				return false;
			}
		} else if (o.getOpCode() == Node.JOINKEY || o.getOpCode()==Node.LEFTJOIN) {
			if(!(o.getObject() instanceof BinaryOperand)){
				return true;
			}
			BinaryOperand bo=(BinaryOperand) o.getObject();
			if(QueryUtils.getJoinColumnFromOperand(o, bo, 0)==null ||QueryUtils.getJoinColumnFromOperand(o, bo, 1)==null){
				return true;
			}
			else if(QueryUtils.getJoinColumnFromOperand(o, bo, 0).equals(c)||QueryUtils.getJoinColumnFromOperand(o, bo, 1).equals(c)){
				return false;
			}
			else return true;
		}
		else {
			return false;
		}
	}

	private int getRetainsPartition(int a) {
		if (a == Node.PROJECT || a == Node.SELECT || a == Node.BASEPROJECT) {
			return 0;
		} else {
			return -1;
		}

	}

	private void createProjections(Node e) {
		for (String t : this.refCols.keySet()) {
			if(n2a.contailsAliasForBaseTable(t)){
			for (String alias : n2a.getAllAliasesForBaseTable(t)) {
				Node table = new Node(Node.OR);
				table.setObject(new Table(t, alias));
				Node tableInHashes = hashes.get(table.getHashId());
				if (tableInHashes == null) {
					// System.out.println("not found");
				} else {
					Node project;
					Node orNode;
					if (tableInHashes.getParents().size() == 1
							&& tableInHashes.getFirstParent().getOpCode() == Node.BASEPROJECT) {
						project = tableInHashes.getFirstParent();
						orNode = project.getFirstParent();
						Projection prj = (Projection) project.getObject();
						hashes.remove(project.getHashId());
						for (String c : refCols.get(t)) {
							Column toAdd = new Column(alias, c);
							if (!prj.getAllColumnRefs().contains(toAdd))
								prj.addOperand(new Output(alias + "_" + c, toAdd));
						}
						this.hashes.put(project.getHashId(), project);
						this.hashes.put(orNode.getHashId(), orNode);

					} else {
						orNode = new Node(Node.OR);
						orNode.setObject(new Table("table" + Util.createUniqueId(), null));
						project = new Node(Node.AND, Node.BASEPROJECT);
						orNode.addChild(project);
						Projection prj = new Projection();
						for (String c : refCols.get(t)) {
							prj.addOperand(new Output(alias + "_" + c, new Column(alias, c)));
						}
						project.setObject(prj);
						Set<Node> toRecompute = new HashSet<Node>();
						while (!tableInHashes.getParents().isEmpty()) {
							Node p = tableInHashes.getFirstParent();
							tableInHashes.getParents().remove(0);
							int childNo = p.getChildren().indexOf(tableInHashes);
							this.hashes.remove(p.getHashId());
							p.removeChild(tableInHashes);
							p.addChildAt(orNode, childNo);
							toRecompute.add(p);
							// this.hashes.put(p.getHashId(), p);
						}
						project.addChild(tableInHashes);
						this.hashes.put(project.getHashId(), project);
						this.hashes.put(orNode.getHashId(), orNode);
						for (Node r : toRecompute) {
							this.hashes.put(r.getHashId(), r);
							// recompute parents?

							setParentsNeedRecompute(r);
						}
					}

					project.addDescendantBaseTable(alias);
					orNode.addDescendantBaseTable(alias);

				}
			}
			}
			else{
				log.debug("Cannot create projection for base table "+t+". Probably table"
						+ "from nested subquery.");
			}
		}
	}

	private void setParentsNeedRecompute(Node r) {
		for (Node p : r.getParents()) {
			hashes.remove(p.getHashId());
			p.computeHashID();
			hashes.put(p.getHashId(), p);
			setParentsNeedRecompute(p);
		}

	}

	private SinglePlan getBestPlanPrunedNoMat(Node e, Column c, double limit, double repCost,
			EquivalentColumnClasses partitionRecord, Memo memo) {
		if (limits.containsKey(e)) {
			if (limits.get(e) > limit - repCost) {
				return null;
			}
		}
		MemoKey ec = new MemoKey(e, c);
		SinglePlan resultPlan;
		if (memo.containsMemoKey(ec)) {
			PartitionedMemoValue pmv = (PartitionedMemoValue) memo.getMemoValue(ec);
			if (pmv.getRepCost() > repCost) {
				resultPlan = searchForBestPlanPrunedNoMat(e, c, limit, repCost, partitionRecord, memo);
			} else {
				resultPlan = memo.getMemoValue(ec).getPlan();
				partitionRecord.setLastPartitioned(pmv.getDlvdPart());
			}

		} else {
			resultPlan = searchForBestPlanPrunedNoMat(e, c, limit, repCost, partitionRecord, memo);
		}
		if (resultPlan != null && resultPlan.getCost() < limit) {
			return resultPlan;
		} else {
			return null;
		}
	}

	private SinglePlan searchForBestPlanPrunedNoMat(Node e, Column c, double limit, double repCost,
			EquivalentColumnClasses partitionRecord, Memo memo) {

		if (!e.getObject().toString().startsWith("table")) {
			// base table
			SinglePlan r = new SinglePlan(0);
			memo.put(e, r, c, repCost, true, null);
			return r;
		}

		SinglePlan resultPlan = null;
		double repartitionCost = 0;
		if (c != null) {
			repartitionCost = nce.estimateRepartition(e, c);
		}
		/*
		 * PartitionCols e2partCols = new PartitionCols(); Node np = new
		 * Node(Node.OR); np.setPartitionedOn(e2partCols);
		 * e2partCols.addColumn(c); //
		 * if(e.getObject().toString().startsWith("table")){ // np.setObject(new
		 * Table("table" + Util.createUniqueId(), null));} // else{
		 * np.setObject(e.getObject()); // }
		 */
		// memo.put(ec, np);
		// e2Plan;
		// for (Node o : e.getChildren()) {
		for (int k = 0; k < e.getChildren().size(); k++) {
			EquivalentColumnClasses e2RecordCloned = partitionRecord.shallowCopy();
			Node o = e.getChildAt(k);

			Double opCost = nce.getCostForOperator(o);
			SinglePlan e2Plan = null;
			// this must go after algorithmic implementation
			double newLimit = limit - opCost;
			if (newLimit < 0) {
				continue;
			}
			if (o.getOpCode() == Node.JOIN) {
				NonUnaryWhereCondition join = (NonUnaryWhereCondition) o.getObject();
				e2RecordCloned.mergePartitionRecords(join);
			}

			double algLimit;
			EquivalentColumnClasses algRecordCloned;
			int cComesFromChildNo = -1;
			for (int m = 0; m < o.getAlgorithmicImplementations().length; m++) {

				int a = o.getAlgorithmicImplementations()[m];

				int retainsPartition = -1;
				retainsPartition = getRetainsPartition(a);
				PartitionCols returnedPt = null;
				algRecordCloned = e2RecordCloned.shallowCopy();
				algLimit = newLimit;

				SinglePlan algPlan = new SinglePlan(opCost);
				algPlan.setChoice(k);
				if (c != null && guaranteesResultPtnedOn(a, o, c)) {
					algRecordCloned.setLastPartitioned(algRecordCloned.getClassForColumn(c));
				}
				for (int i = 0; i < o.getChildren().size(); i++) {
					EquivalentColumnClasses oRecord = algRecordCloned.shallowCopy();
					Node e2 = o.getChildAt(i);
					if (m == 0 && c != null && cComesFromChildNo < 0) {
						if (e2.isDescendantOfBaseTable(c.getAlias())) {
							cComesFromChildNo = i;
						}
					}

					// double minRepCost = repCost < repartitionCost ?
					// repCost:repCost;
					Column c2 = getPartitionRequired(a, o, i);
					Double c2RepCost = 0.0;
					if (c2 != null) {
						c2RepCost = nce.estimateRepartition(e2, c2);
					}

					if (c == null || cComesFromChildNo != i || guaranteesResultPtnedOn(a, o, c)) {
						// algPlan.append(getBestPlan(e2, c2, memo, algLimit,
						// c2RepCost, cel, partitionRecord, toMatAlg));
						SinglePlan t = getBestPlanPrunedNoMat(e2, c2, algLimit, c2RepCost, oRecord, memo);
						if (t == null) {
							continue;
						}
						algPlan.addInputPlan(e2, c2);
						algPlan.increaseCost(t.getCost());
						algLimit -= t.getCost();
					} else if (guaranteesResultNotPtnedOn(a, o, c)) {

						if (repartitionCost < repCost) {
							algPlan.addRepartitionBeforeOp(c);
							algLimit -= repartitionCost;

							if (algLimit < 0) {
								continue;
							}
							algPlan.increaseCost(repartitionCost);
							// algPlan.getPath().addOption(-1);
							oRecord.setClassRepartitioned(c, true);

						} else {
							algLimit -= repCost;
							if (algLimit < 0) {
								continue;
							}
							algPlan.increaseCost(repCost);
							oRecord.setClassRepartitioned(c, false);
						}
						// algPlan.append(getBestPlan(e2, c2, memo, algLimit,
						// c2RepCost, cel, partitionRecord, toMatAlg));
						SinglePlan t = getBestPlanPrunedNoMat(e2, c2, algLimit, c2RepCost, oRecord, memo);
						if (t == null) {
							continue;
						}
						algPlan.addInputPlan(e2, c2);
						algPlan.increaseCost(t.getCost());
						algLimit -= t.getCost();
					} else {
						// algPlan.append(getBestPlan(e2, c, memo, algLimit,
						// c2RepCost, cel, partitionRecord, toMatAlg));
						SinglePlan t = getBestPlanPrunedNoMat(e2, c, algLimit, c2RepCost, oRecord, memo);
						if (t == null) {
							continue;
						}
						algPlan.addInputPlan(e2, c);
						algPlan.increaseCost(t.getCost());
						algLimit -= t.getCost();
						if (oRecord.getLast() == null
								|| (!oRecord.getLast().contains(c) && repartitionCost < repCost)) {

							// here do not add repCost. it has been added before
							algPlan.addRepartitionBeforeOp(c);
							// algPlan.getPath().addOption(-1);
							oRecord.setClassRepartitioned(c, true);
							// algLimit -= repartitionCost;
						}
						// algLimit -= algPlan.getCost();
					}
					if (c2 != null) {
						if (oRecord.getLast() == null || !oRecord.getLast().contains(c2)) {
							// algPlan.getPath().addOption(-1);
							memo.getMemoValue(new MemoKey(e2, c2)).getPlan().addRepartitionBeforeOp(c2);
							// algPlan.addRepartitionAfterOp(i, c2);
							if (algPlan.getRepartitionBeforeOp() != null) {
								oRecord.setClassRepartitioned(c2, false);
							} else {
								oRecord.setClassRepartitioned(c2, true);
							}
							algLimit -= c2RepCost;
							if (algLimit < 0) {
								continue;
							}
							algPlan.increaseCost(c2RepCost);
						}
					}
					// double e2PlanCost = algPlan.getCost();
					if (c != null) {
						if (oRecord.getLast() == null || !oRecord.getLast().contains(c)) {
							algLimit -= repartitionCost;
							if (algLimit < 0) {
								continue;
							}
							// e2PlanCost += repartitionCost;
						}
					}
					// algLimit -= e2PlanCost;
					// algRecordCloned.addClassesFrom(oRecord);
					if (i == retainsPartition) {
						returnedPt = oRecord.getLast();
					}
				}
				if (returnedPt != null) {
					algRecordCloned.setLastPartitioned(returnedPt);
				}
				if (e2Plan == null || algPlan.getCost() < e2Plan.getCost()) {
					e2Plan = algPlan;
					// toMatE2.clear();
					e2RecordCloned = algRecordCloned;
				}
			}
			if (resultPlan == null || e2Plan.getCost() < resultPlan.getCost()) {
				resultPlan = e2Plan;
				// toMaterialize.clear();
				partitionRecord.copyFrom(e2RecordCloned);
				memo.put(e, resultPlan, c, repCost, e2RecordCloned.getLast(), null);
				if (!e.getParents().isEmpty() && (e.getParents().get(0).getOpCode() == Node.UNION
						|| e.getParents().get(0).getOpCode() == Node.UNIONALL)) {

					limit = resultPlan.getCost();
					System.out.println("prune: " + e.getObject() + "with limit:" + limit);
				}
			}
		}
		if (resultPlan == null) {
			System.out.println("pruned!!!");
			limits.put(e, limit - repCost);
		}
		return resultPlan;

	}

	public void setImportExternal(boolean b) {
		this.importExternal = b;
	}

	public NamesToAliases getN2a() {
		return n2a;
	}

	public void setN2a(NamesToAliases n2a) {
		this.n2a = n2a;
	}

	public String getDotPrint() {
		Set<Node> visited=new HashSet<Node>();
		return root.dotPrint(visited).toString();
	}

	public void addRefCols(Map<String, Set<String>> refCols2) {
		for(String t:refCols2.keySet()){
			if(refCols.containsKey(t)){
				refCols.get(t).addAll(refCols2.get(t));
			}
			else{
				refCols.put(t, refCols2.get(t));
			}
		}
		
	}
}
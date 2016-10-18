/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package madgik.exareme.master.queryProcessor.decomposer.federation;

import madgik.exareme.master.queryProcessor.decomposer.DecomposerUtils;
import madgik.exareme.master.queryProcessor.decomposer.dag.Node;
import madgik.exareme.master.queryProcessor.decomposer.dag.PartitionCols;
import madgik.exareme.master.queryProcessor.decomposer.dag.ResultList;
import madgik.exareme.master.queryProcessor.decomposer.query.*;
import madgik.exareme.master.queryProcessor.decomposer.util.Util;
import madgik.exareme.utils.properties.AdpDBProperties;

import java.util.*;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

/**
 * @author dimitris
 */
public class SinlgePlanDFLGenerator {

	private Node root;
	private int partitionNo;
	private Memo memo;
	private Map<HashCode, madgik.exareme.common.schema.Table> registry;
	private NamesToAliases n2a;
	private Map<String, Set<Column>> matResultUsedCols;
	private boolean useSIP;
	private SipStructure sipStruct;
	private boolean useCache;
	private boolean addIndicesToMatQueries = false;
	private SipToUnions sipToUnions;
	private int unionNo;

	private static final org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(SinlgePlanDFLGenerator.class);

	SinlgePlanDFLGenerator(Node n, int partNo, Memo m, Map<HashCode, madgik.exareme.common.schema.Table> r, boolean useCache) {
		this.root = n;
		this.partitionNo = partNo;
		this.memo = m;
		this.registry = r;
		if (addIndicesToMatQueries) {
			matResultUsedCols = new HashMap<String, Set<Column>>();
		}
		this.useCache=useCache;
	}

	public ResultList generate() {
		ResultList qs = new ResultList();
		MemoKey rootkey = new MemoKey(root, null);

		// printPlan(rootkey);
		qs.setCurrent(new SQLQuery());
		unionNo = 0;
		try {
			if (partitionNo == 1) {
				if (DecomposerUtils.PUSH_PROCESSING){
					combineOperatorsAndOutputQueriesCentralizedPush(rootkey, qs, new HashMap<MemoKey, SQLQuery>(), false);
				}
				else{
				combineOperatorsAndOutputQueriesCentralized(rootkey, qs, new HashMap<MemoKey, SQLQuery>());
				}
			} else if (DecomposerUtils.PUSH_PROCESSING) {
				combineOperatorsAndOutputQueriesPush(rootkey, qs, new HashMap<MemoKey, SQLQuery>(), false);
			} else {
				combineOperatorsAndOutputQueries(rootkey, qs, new HashMap<MemoKey, SQLQuery>());
			}
		} catch (CloneNotSupportedException clone) {
			log.error("Could not generate plan " + clone.getMessage());
		}
		// for (SQLQuery q : qs) {
		// System.out.println(q.toDistSQL() + "\n");
		// }

		if (qs.isEmpty()) {
			if (useCache && registry.containsKey(rootkey.getNode().getHashId())) {
				String tableInCache = registry.get(rootkey.getNode().getHashId()).getName();
				SQLQuery res = new SQLQuery();
				res.addInputTable(new Table(tableInCache, tableInCache));
				res.setExistsInCache(true);
				qs.add(res);
			} else {
				log.error("Decomposer produced empty result");
			}
		} /*
			 * else {
			 * 
			 * for (int i = 0; i < qs.size(); i++) { if
			 * (qs.get(i).existsInCache()) { qs.remove(i); i--; } } }
			 */

		/*
		 * for (int i = 0; i < qs.size(); i++) { SQLQuery q = qs.get(i); if
		 * (q.getOutputs().isEmpty()) { for (Table t : q.getInputTables()) {
		 * boolean found = false; for (int j = 0; j < i; j++) { SQLQuery q2 =
		 * qs.get(j); if (q2.getTemporaryTableName().equals(t.getName())) { for
		 * (Output o2 : q2.getOutputs()) { q.addOutput(t.getAlias(),
		 * o2.getOutputName()); } found = true; break; } } // find Outputs from
		 * registry if (!found) { for (madgik.exareme.common.schema.Table table
		 * : registry.values()) { if (table.getName().equals(t.getName())) {
		 * String schema = table.getSqlDefinition(); schema =
		 * schema.substring(schema.indexOf("(")); schema = schema.replace(")",
		 * ""); String[] outputs = schema.split(" "); for (int k = 0; i <
		 * outputs.length; i++) { String output = outputs[k]; if
		 * (!output.equals(" ") && !output.toUpperCase().equals("INTEGER") &&
		 * !output.toUpperCase().equals("INT") &&
		 * !output.toUpperCase().equals("NUM") &&
		 * !output.toUpperCase().equals("REAL") &&
		 * !output.toUpperCase().equals("BLOB") &&
		 * !output.toUpperCase().equals("NULL") &&
		 * !output.toUpperCase().equals("NUMERIC") &&
		 * !output.toUpperCase().equals("TEXT")) { q.addOutput(t.getAlias(),
		 * output); } } found = true; break; } } } if(!found){ log.error(
		 * "Input table not found:"+t.getName()); }
		 * 
		 * } } }
		 */

		if (addIndicesToMatQueries) {
			Map<Column, Column> correspondingCols = new HashMap<Column, Column>();
			Set<Column> toCreateIndex = new HashSet<Column>();
			for (int i = 0; i < qs.size() - 1; i++) {
				SQLQuery q = qs.get(i);

				for (Column c : q.getAllColumns()) {
					if (correspondingCols.keySet().contains(c)) {
						Column cor = correspondingCols.get(c);
						c.setName(cor.getName());
						c.setAlias(cor.getAlias());
						c.setBaseTable(cor.getBaseTable());
					}
				}
				if (matResultUsedCols.keySet().contains(q.getTemporaryTableName())) {
					EquivalentColumnClasses ec = new EquivalentColumnClasses();
					for (NonUnaryWhereCondition nuwc : q.getBinaryWhereConditions()) {
						if (nuwc.getOperator().equals("=") && nuwc.getLeftOp() instanceof Column
								&& nuwc.getRightOp() instanceof Column) {
							ec.mergePartitionRecords(nuwc);
						}
					}
					for (PartitionCols eqCols : ec.getPartitionSets()) {
						Column representantive = eqCols.getFirstCol();
						for (Column col : eqCols.getColumns()) {
							if (!col.equals(representantive)) {
								for (int j = 0; j < q.getOutputs().size(); j++) {
									Output o = q.getOutputs().get(j);
									if (o.getObject().equals(col)) {
										q.getOutputs().remove(o);
										j--;
									}
								}
								correspondingCols.put(
										new Column(q.getTemporaryTableName(), col.getBaseTable() + "_" + col.getName()),
										new Column(q.getTemporaryTableName(), representantive.getName(),
												representantive.getBaseTable()));
								correspondingCols.put(
										new Column(q.getTemporaryTableName(), col.getName(), col.getBaseTable()),
										new Column(q.getTemporaryTableName(), representantive.getName(),
												representantive.getBaseTable()));

							}
						}
					}

					for (Column indexCandidate : matResultUsedCols.get(q.getTemporaryTableName())) {
						Column renamed = new Column(q.getTemporaryTableName(), indexCandidate.getName(),
								indexCandidate.getAlias());
						Column cor = correspondingCols.get(renamed);
						if (cor == null) {
							cor = renamed;
						}
						toCreateIndex.add(cor);
					}
					System.out.println("Indexes::" + toCreateIndex.toString());
				}
			}
			for (Column i : toCreateIndex) {
				SQLQuery indexQuery = new SQLQuery();
				indexQuery.setIsCreateIndex();
				indexQuery.setSQL("distributed create index " + i.getName() + "_" + i.getAlias() + " on " + i.getAlias()
						+ "(" + i.getBaseTable() + "_" + i.getName() + ")");
				qs.add(qs.size() - 1, indexQuery);
			}
		}

		if (DecomposerUtils.REMOVE_OUTPUTS) {
			log.debug("Removing Outputs...");
			if (qs.size() > 1) {
				List<SQLQuery> unions = qs.get(qs.size() - 1).getUnionqueries();
				for (int i = qs.size() - 2; i > -1; i--) {
					//Set<SQLQuery> queriesUsingQ=new HashSet<SQLQuery>();
					SQLQuery q = qs.get(i);
					if (unions.contains(q)) {
						continue;
					}
					//log.debug("removing from:"+q.getTemporaryTableName());
					//log.debug("number:"+i);
					List<Column> outputs = new ArrayList<Column>();
					for (Output o : q.getOutputs()) {
						outputs.add(new Column(q.getTemporaryTableName(), o.getOutputName()));
					}
					//log.debug("outputs:"+outputs);
					for (int j = qs.size() - 1; j > i; j--) {
						if(qs.get(j).containsIputTable(q.getTemporaryTableName())){
						if(qs.get(j).getOutputs().isEmpty()){
							outputs.clear();
							break;
						}
						//else{
						///	queriesUsingQ.add(qs.get(j));
						//}
						}
						List<Column> cols = qs.get(j).getAllColumns();
						for (Column c2 : cols) {
							//log.debug(c2);
							if (c2.getBaseTable() != null) {
								outputs.remove(new Column(c2.getAlias(), c2.getBaseTable() + "_" + c2.getName()));
							} else {
								outputs.remove(c2);
							}
						}
						if (outputs.isEmpty()) {
							break;
						}
					}
					Set<HashCode> removedOutputs=new HashSet<HashCode>();
					for (Column c3 : outputs) {
						for (int k = 0; k < q.getOutputs().size(); k++) {
							Output o = q.getOutputs().get(k);
							if (o.getOutputName().equals(c3.getName())) {
								Output out=q.getOutputs().get(k);
								log.debug("removing output:"+o);
								q.getOutputs().remove(k);
								removedOutputs.add(out.getHashID());
								//q.setHashId(null);
								k--;
							}
						}
					}
					if(!removedOutputs.isEmpty()&&q.getHashId()!=null){
						HashCode outputsAll=Hashing.combineUnordered(removedOutputs);
						List<HashCode> newHashForQuery=new ArrayList<HashCode>();
						newHashForQuery.add(q.getHashId());
						newHashForQuery.add(Hashing.sha1().hashBytes("removing outputs".getBytes()));
						newHashForQuery.add(outputsAll);
						q.setHashId(Hashing.combineOrdered(newHashForQuery));
						/*for(SQLQuery using:queriesUsingQ){
							List<HashCode> newHashForQueryUsing=new ArrayList<HashCode>();
							newHashForQueryUsing.add(using.getHashId());
							newHashForQueryUsing.add(Hashing.sha1().hashBytes("removing outputs".getBytes()));
							newHashForQueryUsing.add(outputsAll);
							using.setHashId(Hashing.combineOrdered(newHashForQueryUsing));
						}*/
						if (useCache && registry.containsKey(newHashForQuery)) {
							String tableInRegistry = registry.get(newHashForQuery).getName();
							String qName= q.getTemporaryTableName();
							for (int j = qs.size() - 1; j > i; j--) {
								for(Table t:qs.get(j).getInputTables()){
									if(t.getName().equals(qName)){
										t.setName(tableInRegistry);
									}
								}
							}
							qs.remove(i);
						}
						
					}
					if(removedOutputs.isEmpty()&&q.existsInCache()){
						//remove q and replace with the cached tablename
						String tablename=q.getInputTables().get(0).getName();
						String qName= q.getTemporaryTableName();
						for (int j = qs.size() - 1; j > i; j--) {
							for(Table t:qs.get(j).getInputTables()){
								if(t.getName().equals(qName)){
									t.setName(tablename);
								}
							}
						}
						qs.remove(i);
						
					}
				}
			}
		}

		if (useSIP) {
			boolean makeQueryForEachSip = false;
			System.out.println("initial size:" + qs.size());
			if (makeQueryForEachSip) {
				Map<String, Set<SQLQuery>> queriesToSip = new HashMap<String, Set<SQLQuery>>();
				for (int i = 0; i < qs.size() - 1; i++) {
					SQLQuery q = qs.get(i);
					Set<SipJoin> sis = q.getSipInfo();
					if (sis != null && sis.size() > 0) {
						// String si = sis.iterator().next().getSipName();
						boolean most = true;
						String si = null;
						if (most) {
							si = q.getMostProminentSipjoin();
						} else {
							si = q.getLeastProminentSipjoin();
						}
						if (sis.size() > 1) {
							System.out.println("size>1:" + sis.size());
							System.out.println(q.getTemporaryTableName());
						}
						if (queriesToSip.containsKey(si)) {
							queriesToSip.get(si).add(q);
						} else {
							Set<SQLQuery> s = new HashSet<SQLQuery>();
							s.add(q);
							queriesToSip.put(si, s);
						}
					}
				}
				SQLQuery union = qs.get(qs.size() - 1);

				for (String si : queriesToSip.keySet()) {
					System.out.println("no of sip:" + queriesToSip.get(si).size());
					if (queriesToSip.get(si).size() == 1) {
						System.out.println("si with one query!" + queriesToSip.get(si));
						/*
						 * SQLQuery s = queriesToSip.get(si).iterator().next();
						 * for (int t = 0; t < s.getInputTables().size(); t++) {
						 * Table tbl = s.getInputTables().get(t); if
						 * (tbl.getName().equals("siptable")) {
						 * s.getInputTables().remove(t); t--; } } //
						 * s.getInputTables().remove(s.getInputTables().size() -
						 * 1); for (NonUnaryWhereCondition nuwc :
						 * s.getBinaryWhereConditions()) { if
						 * (nuwc.getAllColumnRefs().contains(new Column(si,
						 * "x"))) { s.getBinaryWhereConditions().remove(nuwc);
						 * break; } // qs.add(qs.size()-1, s); }
						 */
						continue;
					}
					// SQLQuery mostTables = null;
					// int noTables = 0;
					// int lastQ = 0;

					for (SQLQuery s : queriesToSip.get(si)) {
						union.getUnionqueries().remove(s);
						qs.remove(s);
						s.addSipJoin(si);
					}
					SQLQuery toReplace = new SQLQuery();
					StringBuffer toRepl = new StringBuffer();
					toRepl.append("select * from (");
					String separator = "";

					for (SQLQuery s : queriesToSip.get(si)) {
						if (s.sipJoinIsLast()) {
							toRepl.append(separator);
							toRepl.append(s.toSipSQL());
							separator = " UNION ALL ";
							// now setting union all to avoid not necessary
							// processing
							// duplicate elimination will occur in the final
							// query
						}

					}
					for (SQLQuery s : queriesToSip.get(si)) {
						if (!s.sipJoinIsLast()) {
							toRepl.append(separator);
							toRepl.append(s.toSipSQL());
							separator = " UNION ALL ";
							// now setting union all to avoid not necessary
							// processing
							// duplicate elimination will occur in the final
							// query
						}

					}
					qs.add(qs.size() - 1, toReplace);
					// toRepl.append(separator);
					// toRepl.append(mostTables.toSipSQL(true));
					toRepl.append(")");
					toReplace.setSQL(toRepl.toString());
					toReplace.setStringSQL();
					union.getUnionqueries().add(toReplace);
					if (qs.size() == 2) {
						toReplace.setSQL(toRepl.toString().replaceAll(" UNION ALL ", " UNION "));
						toReplace.setTemporary(false);
						qs.remove(union);
					}
				}

			}
			if (!makeQueryForEachSip) {
				Map<String, Boolean> sips = new HashMap<String, Boolean>();
				Set<String> createVtables = new HashSet<String>();
				for (int i = 0; i < qs.size() - 1; i++) {
					// check to remove sips that are used only once
					SQLQuery q = qs.get(i);
					Set<SipJoin> sis = q.getSipInfo();
					if (sis != null && sis.size() > 0) {
						for (SipJoin sj : sis) {
							if (sips.containsKey(sj.getSipName())) {
								sips.put(sj.getSipName(), Boolean.TRUE);
								createVtables
										.add("create virtual table " + sj.getSipName() + " using unionsiptext; \n");
							} else {
								sips.put(sj.getSipName(), Boolean.FALSE);
							}
						}
					}
				}
				SQLQuery toReplace = new SQLQuery();
				StringBuffer toRepl = new StringBuffer();

				String separator = "";
				if (qs.size() > 1) {
					toRepl.append("select * from (");
					for (int k = 0; k < qs.size() - 1; k++) {
						SQLQuery s = qs.get(k);
						s.adddAllSips(sips);
						toRepl.append(separator);
						toRepl.append(s.toSipSQL());

						separator = " UNION ";

					}

					// toRepl.append(separator);
					// toRepl.append(mostTables.toSipSQL(true));
					toRepl.append(")");
				} else {
					SQLQuery s = qs.get(0);
					toRepl.append(s.toSipSQL());
				}
				toReplace.setSQL(toRepl.toString());
				toReplace.setStringSQL();
				for (String v : createVtables) {
					toReplace.appendCreateSipTables(v);
				}
				System.out.println(toRepl.toString());
				qs.clear();
				qs.add(toReplace);

			}

			System.out.println("final size:" + qs.size());
		}

		// remove not needed columns from nested subqueries

		SQLQuery last = qs.get(qs.size() - 1);
		int merge=DecomposerUtils.MERGE_UNIONS;
		if (merge>0 && last.getUnionqueries().size() > merge) {

			SQLQuery current = new SQLQuery();
			// random hash, to fix
			current.setHashId(Hashing.sha1().hashBytes(current.getTemporaryTableName().getBytes()));
			current.setIsUnionAll(true);
			current.setHasUnionRootNode(last.isHasUnionRootNode());
			List<SQLQuery> allUnions = last.getUnionqueries();
			last.setUnionqueries(new ArrayList<SQLQuery>());
			last.getUnionqueries().add(current);
			qs.add(qs.size() - 1, current);
			for (int i = 0; i < allUnions.size(); i++) {
				if (i % merge == (merge-1)) {
					current = new SQLQuery();
					current.setIsUnionAll(true);
					current.setHasUnionRootNode(last.isHasUnionRootNode());
					// random hash, to fix
					current.setHashId(Hashing.sha1().hashBytes(current.getTemporaryTableName().getBytes()));
					qs.add(qs.size() - 1, current);
					last.getUnionqueries().add(current);
				}
				current.getUnionqueries().add(allUnions.get(i));
			}
		}
		if(this.partitionNo>1){
			for(SQLQuery s:qs){
				s.setNumberOfPartitions(partitionNo);
			}
		}
		return qs;
	}

	private void printPlan(MemoKey key) {
		if (this.memo.containsMemoKey(key)) {
			System.out.println("memo key: " + key);

			System.out.println("mater::" + memo.getMemoValue(key).isMaterialised());
			// System.out.println("plan: " +
			// memo.getMemoValue(key).getPlan().getPath().toString());
			System.out.println("reps: ");
			// while (true) {
			Column r = null;
			try {
				r = memo.getMemoValue(key).getPlan().getRepartitionBeforeOp();
			} catch (java.lang.IndexOutOfBoundsException f) {
			}
			// break;
			// }
			System.out.println("repBef:" + r);
			for (int i = 0; i < 2; i++) {
				System.out.println("repAfter:" + memo.getMemoValue(key).getPlan().getRepartitionAfterOp(i));
			}
			for (int i = 0; i < 99999999; i++) {
				MemoKey p2;
				try {
					p2 = memo.getMemoValue(key).getPlan().getInputPlan(i);
				} catch (java.lang.IndexOutOfBoundsException f) {
					break;
				}
				printPlan(p2);

			}
		} else {
			System.out.println("not contains: " + key.toString());
		}

	}

	private void combineOperatorsAndOutputQueries(MemoKey k, ResultList tempResult, HashMap<MemoKey, SQLQuery> visited)
			throws CloneNotSupportedException {

		SQLQuery current = tempResult.getCurrent();
		MemoValue v = memo.getMemoValue(k);
		SinglePlan p = v.getPlan();
		Node e = k.getNode();

		if (useCache && registry.containsKey(e.getHashId()) && e.getHashId() != null) {
			String tableInRegistry = registry.get(e.getHashId()).getName();
			Table t = new Table(tableInRegistry, tableInRegistry);
			tempResult.setLastTable(t);
			// tempResult.trackBaseTableFromQuery(t.getAlias(), t.getAlias());
			SQLQuery cached = new SQLQuery();
			cached.setTemporaryTableName(tableInRegistry);
			visited.put(k, cached);
			cached.setHashId(e.getHashId());
			// current.setTemporaryTableName(tableInRegistry);
			current.setExistsInCache(true);

			for (String alias : e.getDescendantBaseTables()) {
				tempResult.trackBaseTableFromQuery(alias, tableInRegistry);
			}

			return;
		}

		if (visited.containsKey(k) && visited.get(k).isMaterialised()) {
			tempResult.setLastTable(visited.get(k));

			tempResult.remove(current);
			for (String alias : e.getDescendantBaseTables()) {
				tempResult.trackBaseTableFromQuery(alias, tempResult.getLastTable().getAlias());
			}
			return;
		}

		if (!e.getObject().toString().startsWith("table")) {
			Table t = (Table) k.getNode().getObject();
			tempResult.setLastTable(t);
			// visited.put(k, current);

			// if (memo.getMemoValue(k).isMaterialised()) {
			// current.setMaterialised(true);
			// current.setHashId(e.getHashId());
			// }
			tempResult.trackBaseTableFromQuery(t.getAlias(), t.getAlias());
			return;
		}
		Column repBefore = null;
		if (p.getRepartitionBeforeOp() != null) {
			repBefore = new Column(null, p.getRepartitionBeforeOp().getName(), p.getRepartitionBeforeOp().getAlias());
		}
		Node op = e.getChildAt(p.getChoice());
		SQLQuery old = current;
		if (memo.getMemoValue(k).isMaterialised()) {

			SQLQuery q2 = new SQLQuery();
			q2.setHashId(e.getHashId());
			tempResult.setCurrent(q2);
			current = q2;
			current.setMaterialised(true);
			visited.put(k, current);
		}

		if (repBefore != null) {
			current.setRepartition(repBefore, partitionNo);
			current.setPartition(repBefore);
		}

		if (op.getOpCode() == Node.PROJECT || op.getOpCode() == Node.BASEPROJECT) {
			String inputName;

			Projection prj = (Projection) op.getObject();
			// current = q2;
			// q2.setPartition(repAfter, partitionNo);

			for (Output o : prj.getOperands()) {
				current.getOutputs().add(o.clone());
			}
			current.setOutputColumnsDinstict(prj.isDistinct());
			// current.getOutputs().addAll(prj.getOperands());

			combineOperatorsAndOutputQueries(p.getInputPlan(0), tempResult, visited);
			// visited.put(p.getInputPlan(j), q2);

			if (op.getOpCode() == Node.PROJECT) {
				// if we have a final result, remove previous projections
				for (int l = 0; l < current.getOutputs().size(); l++) {
					Output o = current.getOutputs().get(l);
					boolean exists = false;
					for (Output pr : prj.getOperands()) {
						if (pr.getOutputName().equals(o.getOutputName())) {
							exists = true;
							break;
						}
					}
					if (!exists) {
						current.getOutputs().remove(o);
						l--;
					}
				}
			}
			if (tempResult.getLastTable() != null) {
				inputName = tempResult.getLastTable().getAlias();
				if (inputName != current.getTemporaryTableName()
						&& !current.getInputTables().contains(tempResult.getLastTable())) {
					current.addInputTable(tempResult.getLastTable());
					for (Output o : current.getOutputs()) {
						for (Column c : o.getObject().getAllColumnRefs()) {
							{
								if (op.isDescendantOfBaseTable(c.getAlias())) {
									o.getObject().changeColumn(c, new Column(
											tempResult.getQueryForBaseTable(c.getAlias()), c.getName(), c.getAlias()));
								}
							}
						}

					}
				}

			}

		} else if (op.getOpCode() == Node.JOIN) {
			NonUnaryWhereCondition nuwc = (NonUnaryWhereCondition) op.getObject();
			NonUnaryWhereCondition nuwcCloned = nuwc.clone();
			current.addBinaryWhereCondition(nuwcCloned);
			List<String> inputNames = new ArrayList<String>();
			for (int j = 0; j < op.getChildren().size(); j++) {

				combineOperatorsAndOutputQueries(p.getInputPlan(j), tempResult, visited);
				inputNames.add(tempResult.getLastTable().getAlias());
				if (tempResult.getLastTable().getAlias() != current.getTemporaryTableName()
						&& !current.getInputTables().contains(tempResult.getLastTable())) {
					current.addInputTable(tempResult.getLastTable());
					addOutputs(current, tempResult.getLastTable().getAlias(), tempResult);
					for (Output o : current.getOutputs()) {
						for (Column c : o.getObject().getAllColumnRefs()) {
							{
								if (op.getChildAt(j).isDescendantOfBaseTable(c.getAlias())) {
									o.getObject().changeColumn(c, new Column(
											tempResult.getQueryForBaseTable(c.getAlias()), c.getName(), c.getAlias()));
								}
							}
						}

					}
				}
			}

			for (NonUnaryWhereCondition bwc : current.getBinaryWhereConditions()) {

				for (int j = 0; j < op.getChildren().size(); j++) {
					for (Column c : bwc.getAllColumnRefs()) {
					for (Operand o : bwc.getOperands()) {

						//List<Column> cs = o.getAllColumnRefs();
					//	if (!cs.isEmpty()) {
							// not constant
						//	Column c = cs.get(0);
							if (op.getChildAt(j).isDescendantOfBaseTable(c.getAlias())) {
								bwc.changeColumn(c, new Column(tempResult.getQueryForBaseTable(c.getAlias()),
										c.getName(), c.getAlias()));
							}
						}

					}
				}
			}
			// }

			/*
			 * for (Output o : current.getOutputs()) { for (Column c :
			 * o.getObject().getAllColumnRefs()) { { for (int j = 0; j <
			 * op.getChildren().size(); j++) { Node child = op.getChildAt(j); if
			 * (child.isDescendantOfBaseTable(c.getAlias())) {
			 * o.getObject().changeColumn(c, new Column(inputNames.get(j),
			 * c.getName())); } } } // o.getObject().changeColumn(c, c); } }
			 */
			// tempResult.add(current);
		} else if (op.getOpCode() == Node.UNION || op.getOpCode() == Node.UNIONALL) {
			List<SQLQuery> unions = new ArrayList<SQLQuery>();
			if (e.getParents().isEmpty()) {
				current.setHashId(e.getHashId());
			}
			// boolean existsPartitioned = false;
			// boolean existsNonPartitioned = false;
			for (int l = 0; l < op.getChildren().size(); l++) {
				SQLQuery u = new SQLQuery();

				// visited.put(op, current);
				tempResult.setCurrent(u);
				combineOperatorsAndOutputQueries(p.getInputPlan(l), tempResult, visited);
				// visited.put(p.getInputPlan(l), u);
				if (memo.getMemoValue(p.getInputPlan(l)).isMaterialised()) {
					u = tempResult.get(tempResult.getLastTable().getAlias());
					if (u == null && useCache) {
						// it's a table from cache
						u = new SQLQuery();
						u.addInputTable(tempResult.getLastTable());
						u.setExistsInCache(true);
						tempResult.add(u);
					}
				} else {
					// visited.put(p.getInputPlan(l), u);
					tempResult.add(u);
				}

				u.setHashId(p.getInputPlan(l).getNode().getHashId());
				if (useCache && u.existsInCache()) {
					String tableInCache = registry.get(u.getHashId()).getName();
					tempResult.remove(u);
					u = new SQLQuery();
					if (op.getChildren().size() == 1) {
						u.addInputTable(new Table(tableInCache, tableInCache));
						u.setExistsInCache(true);
						tempResult.add(u);
						return;
					}
					u.setTemporaryTableName(tableInCache);
					// u.addInputTable(new Table(tableInCache, tableInCache));
				}
				unions.add(u);
				/*
				 * if (u.getPartition() != null) { existsPartitioned = true;
				 * System.out.println("exists partitioned!");
				 * System.out.println(u.getTemporaryTableName()+":"+u.
				 * getPartition().toString()); } else { System.out.println(
				 * "exists non partitioned!");
				 * System.out.println(u.getTemporaryTableName());
				 * existsNonPartitioned = true; }
				 */
			}
			if (unions.size() > 1) {
				tempResult.add(current);
				current.setUnionqueries(unions);
				if (op.getOpCode() == Node.UNION) {
					current.setUnionAll(false);
				}
				visited.put(k, current);
				current.setHashId(e.getHashId());
				if (memo.getMemoValue(k).isMaterialised()) {
					current.setMaterialised(true);
				}
				// if (existsNonPartitioned && existsPartitioned) {
				// make all unions have the same number of partitions
				if (op.getOpCode() == Node.UNION) {
					SQLQuery first = unions.get(0);
					String ptCol = "c1";
					for (Output a : first.getOutputs()) {
						if (!a.getObject().getAllColumnRefs().isEmpty()) {
							ptCol = a.getOutputName();
							break;
						}
					}

					Column pt = new Column(ptCol, ptCol);
					first.setRepartition(pt, partitionNo);
					first.setPartition(pt);
					for (int un = 1; un < unions.size(); un++) {
						SQLQuery u = unions.get(un);
						u.setRepartition(pt, partitionNo);
						u.setPartition(pt);
					}
				}
			} else {
				visited.put(k, unions.get(0));
				unions.get(0).setHashId(p.getInputPlan(0).getNode().getHashId());
				if (memo.getMemoValue(k).isMaterialised()) {
					unions.get(0).setMaterialised(true);
				}
			}
		} else if (op.getOpCode() == Node.SELECT) {
			combineOperatorsAndOutputQueries(p.getInputPlan(0), tempResult, visited);
			String inputName = tempResult.getLastTable().getAlias();
			Selection s = (Selection) op.getObject();
			Iterator<Operand> it = s.getOperands().iterator();
			while (it.hasNext()) {
				Operand o = it.next();
				if (o instanceof UnaryWhereCondition) {
					UnaryWhereCondition uwc = (UnaryWhereCondition) o;
					UnaryWhereCondition uwcCloned = null;
					uwcCloned = uwc.clone();
					if (!inputName.equals(current.getTemporaryTableName())) {
						Column c = uwcCloned.getAllColumnRefs().get(0);
						uwcCloned.changeColumn(c,
								new Column(tempResult.getQueryForBaseTable(c.getAlias()), c.getName(), c.getAlias()));
					}
					current.addUnaryWhereCondition(uwcCloned);
				} else {
					NonUnaryWhereCondition nuwc = (NonUnaryWhereCondition) o;
					NonUnaryWhereCondition nuwcCloned = nuwc.clone();
					if (!inputName.equals(current.getTemporaryTableName())) {
						for (Column c : nuwcCloned.getAllColumnRefs()) {
							nuwcCloned.changeColumn(c, new Column(tempResult.getQueryForBaseTable(c.getAlias()),
									c.getName(), c.getAlias()));
						}
					}
					current.addBinaryWhereCondition(nuwcCloned);
				}
			}

			// visited.put(p.getInputPlan(j), q2);
			if (tempResult.getLastTable() != null) {
				inputName = tempResult.getLastTable().getAlias();
				if (inputName != current.getTemporaryTableName()
						&& !current.getInputTables().contains(tempResult.getLastTable())) {
					current.addInputTable(tempResult.getLastTable());
					addOutputs(current, tempResult.getLastTable().getAlias(), tempResult);
					for (Output o : current.getOutputs()) {
						for (Column c : o.getObject().getAllColumnRefs()) {
							{
								if (op.isDescendantOfBaseTable(c.getAlias())) {
									o.getObject().changeColumn(c, new Column(
											tempResult.getQueryForBaseTable(c.getAlias()), c.getName(), c.getAlias()));
								}
							}
						}

					}
				}

			}

		} else if (op.getOpCode() == Node.LIMIT) {
			current.setLimit(((Integer) op.getObject()).intValue());
			current.setHashId(p.getInputPlan(0).getNode().getHashId());
			combineOperatorsAndOutputQueries(p.getInputPlan(0), tempResult, visited);
			if (current.getInputTables().isEmpty()) {
				// limit of a query that exists in the cache
				current.addInputTable(tempResult.getLastTable());
			}
			if (tempResult.isEmpty() && e == this.root) {
				// final limit of a table tha exists in cache
				tempResult.add(current);
			}
		} else if (op.getOpCode() == Node.NESTED) {
			// nested is always materialized
			// current.setNested(true);
			combineOperatorsAndOutputQueries(p.getInputPlan(0), tempResult, visited);

		} else if (op.getOpCode() == Node.LEFTJOIN || op.getOpCode() == Node.JOINKEY) {
			Operand leftJoinOp = (Operand) op.getObject();
			current.addJoinOperand(leftJoinOp);
			current.setJoinType("left outer join");
			if (op.getOpCode() == Node.JOINKEY) {
				current.setJoinType("join");
			}
			List<String> inputNames = new ArrayList<String>();
			for (int j = 0; j < op.getChildren().size(); j++) {

				combineOperatorsAndOutputQueries(p.getInputPlan(j), tempResult, visited);
				inputNames.add(tempResult.getLastTable().getAlias());
				if (tempResult.getLastTable().getAlias() != current.getTemporaryTableName()
						&& !current.getInputTables().contains(tempResult.getLastTable())) {
					current.addInputTable(tempResult.getLastTable());
					addOutputs(current, tempResult.getLastTable().getAlias(), tempResult);
					for (Output o : current.getOutputs()) {
						for (Column c : o.getObject().getAllColumnRefs()) {
							{
								if (op.getChildAt(j).isDescendantOfBaseTable(c.getAlias())) {
									o.getObject().changeColumn(c, new Column(
											tempResult.getQueryForBaseTable(c.getAlias()), c.getName(), c.getAlias()));
								}
							}
						}

					}
				}
			}

			for (Operand o : current.getJoinOperands()) {
				for (Column c : o.getAllColumnRefs()) {
					String base = tempResult.getQueryForBaseTable(c.getAlias());
					if (base != null) {
						o.changeColumn(c, new Column(base, c.getName(), c.getAlias()));
					}
				}
			}

		} else if (op.getOpCode() == Node.ORDERBY) {
			combineOperatorsAndOutputQueries(p.getInputPlan(0), tempResult, visited);
			SQLQuery q = tempResult.get(tempResult.getLastTable().getName());
			if (q == null) {
				q = current;
			}
			List<ColumnOrderBy> orderCols = (ArrayList<ColumnOrderBy>) op.getObject();
			q.setOrderBy(orderCols);
	} else if (op.getOpCode() == Node.GROUPBY) {
		combineOperatorsAndOutputQueries(p.getInputPlan(0), tempResult, visited);
		SQLQuery q = tempResult.get(tempResult.getLastTable().getName());
		if (q == null) {
			q = current;
		}
		List<Column> groupCols = (ArrayList<Column>) op.getObject();
		q.setGroupBy(groupCols);

	}else {
			log.error("Unknown Operator in DAG");
		}
		current.setExistsInCache(false);
		if (memo.getMemoValue(k).isMaterialised()) {
			tempResult.add(current);
			tempResult.setLastTable(current);
			current.setHashId(e.getHashId());
			for (String alias : e.getDescendantBaseTables()) {
				tempResult.trackBaseTableFromQuery(alias, tempResult.getLastTable().getAlias());
			}
			tempResult.setCurrent(old);
		}
		if (old != current && old.getRepartition() == null) {
			old.setPartition(current.getPartitionColumn());
		}

	}

	private void combineOperatorsAndOutputQueriesCentralized(MemoKey k, ResultList tempResult,
			HashMap<MemoKey, SQLQuery> visited) throws CloneNotSupportedException {

		SQLQuery current = tempResult.getCurrent();
		MemoValue v = memo.getMemoValue(k);

		SinglePlan p = v.getPlan();
		Node e = k.getNode();
		if (visited.containsKey(k) && visited.get(k).isMaterialised()) {
			tempResult.setLastTable(visited.get(k));
			tempResult.remove(current);
			for (String alias : e.getDescendantBaseTables()) {
				tempResult.trackBaseTableFromQuery(alias, tempResult.getLastTable().getAlias());
			}
			return;
		}

		if (useCache && registry.containsKey(e.getHashId()) && e.getHashId() != null) {
			madgik.exareme.common.schema.Table t=registry.get(e.getHashId());
			SQLQuery dummy=new SQLQuery();
			dummy.setMaterialised(true);
			String tableInRegistry = t.getName();
			List<Output> out=Util.getOutputForTable(t.getSqlDefinition(), tableInRegistry);
			Table r = new Table(tableInRegistry, tableInRegistry);
			dummy.addInputTable(r);
			dummy.setOutputs(out);
			dummy.setOutputColumnsDistinct(true);
			tempResult.add(dummy);
			log.debug("adding dummy query:"+dummy.getTemporaryTableName()+" for table in registry:"+tableInRegistry);
			log.debug("outs:"+out);
			//tempResult.setLastTable();
			// tempResult.trackBaseTableFromQuery(t.getAlias(), t.getAlias());
			//SQLQuery cached = new SQLQuery();
			//cached.setTemporaryTableName(tableInRegistry);
			visited.put(k, dummy);
			//dummy.setHashId(null);
			dummy.setExistsInCache(true);
			// current.setTemporaryTableName(tableInRegistry);
			tempResult.setCurrent(current);
			current.setExistsInCache(true);

			for (String alias : e.getDescendantBaseTables()) {
				tempResult.trackBaseTableFromQuery(alias, dummy.getTemporaryTableName());
			}

			return;
		}

		

		if (!e.getObject().toString().startsWith("table")) {
			Table t = (Table) k.getNode().getObject();
			tempResult.setLastTable(t);
			if (t.isFederated()) {
				// visited.put(k, current);
				if (memo.getMemoValue(k).isMaterialised()) {
					current.setMaterialised(true);
					current.setHashId(e.getHashId());
				}
			}

			tempResult.trackBaseTableFromQuery(t.getAlias(), t.getAlias());

			return;
		}

		Node op = e.getChildAt(p.getChoice());
		SQLQuery old = current;
		if (memo.getMemoValue(k).isMaterialised()) {

			SQLQuery q2 = new SQLQuery();
			q2.setHashId(e.getHashId());

			tempResult.setCurrent(q2);
			current = q2;
			current.setMaterialised(true);
			visited.put(k, current);
		}

		if (op.getOpCode() == Node.PROJECT || op.getOpCode() == Node.BASEPROJECT) {
			String inputName;

			Projection prj = (Projection) op.getObject();
	
			for (Output o : prj.getOperands()) {
				current.getOutputs().add(o.clone());
			}
			current.setOutputColumnsDinstict(prj.isDistinct());

			combineOperatorsAndOutputQueriesCentralized(p.getInputPlan(0), tempResult, visited);

			if (op.getOpCode() == Node.PROJECT) {
				Table t=(Table)e.getObject();
				current.setTemporaryTableName(t.getName());
				for (int l = 0; l < current.getOutputs().size(); l++) {
					Output o = current.getOutputs().get(l);
					boolean exists = false;
					for (Output pr : prj.getOperands()) {
						if (pr.getOutputName().equals(o.getOutputName())) {
							exists = true;
							break;
						}
					}
					if (!exists) {
						current.getOutputs().remove(o);
						l--;
					}
				}

				

			}
			
			if (tempResult.getLastTable() != null) {
				inputName = tempResult.getLastTable().getAlias();
				if (inputName != current.getTemporaryTableName()
						&& !current.getInputTables().contains(tempResult.getLastTable())) {
					current.addInputTableIfNotExists(tempResult.getLastTable());

					for (Output o : current.getOutputs()) {
						for (Column c : o.getObject().getAllColumnRefs()) {
							{
								if (op.isDescendantOfBaseTable(c.getAlias())) {
									o.getObject().changeColumn(c, new Column(
											tempResult.getQueryForBaseTable(c.getAlias()), c.getName(), c.getAlias()));
								}
							}
						}

					}
				}

			}

		} else if (op.getOpCode() == Node.JOIN) {
			NonUnaryWhereCondition nuwc = (NonUnaryWhereCondition) op.getObject();
			NonUnaryWhereCondition nuwcCloned = nuwc.clone();
			nuwcCloned.addRangeFilters(nuwc);
			current.addBinaryWhereCondition(nuwcCloned);
			List<String> inputNames = new ArrayList<String>();
			for (int j = 0; j < op.getChildren().size(); j++) {

				combineOperatorsAndOutputQueriesCentralized(p.getInputPlan(j), tempResult, visited);
				String lastRes = tempResult.getLastTable().getAlias();
				inputNames.add(lastRes);
				if (lastRes != current.getTemporaryTableName()
						&& !current.getInputTables().contains(tempResult.getLastTable())) {
					current.addInputTableIfNotExists(tempResult.getLastTable());
					addOutputs(current, lastRes, tempResult);
					if (addIndicesToMatQueries && p.getInputPlan(j).getNode().isMaterialised()) {
						if (!matResultUsedCols.containsKey(lastRes)) {
							matResultUsedCols.put(lastRes, new HashSet<Column>());
						}
						Set<Column> usedCols = matResultUsedCols.get(lastRes);
						usedCols.add(nuwc.getOp(j).getAllColumnRefs().get(0));
					}
					for (Output o : current.getOutputs()) {
						for (Column c : o.getObject().getAllColumnRefs()) {
							{
								if (op.getChildAt(j).isDescendantOfBaseTable(c.getAlias())) {
									o.getObject().changeColumn(c, new Column(
											tempResult.getQueryForBaseTable(c.getAlias()), c.getName(), c.getAlias()));
									// addOutputIfNotExists(c, tempResult);
								}
							}
						}

					}
				}
			}

			for (NonUnaryWhereCondition bwc : current.getBinaryWhereConditions()) {

				for (int j = 0; j < op.getChildren().size(); j++) {
				//	for (Operand o : bwc.getOperands()) {
						for (Column c : bwc.getAllColumnRefs()) {
					//	List<Column> cs = o.getAllColumnRefs();
					//	if (!cs.isEmpty()) {
							// not constant
						//	Column c = cs.get(0);

							if (op.getChildAt(j).isDescendantOfBaseTable(c.getAlias())) {
								bwc.changeColumn(c, new Column(tempResult.getQueryForBaseTable(c.getAlias()),
										c.getName(), c.getAlias()));
								// addOutputIfNotExists(c, tempResult);
							}
						}

					}
				}
			

		} else if (op.getOpCode() == Node.UNION || op.getOpCode() == Node.UNIONALL) {
			List<SQLQuery> unions = new ArrayList<SQLQuery>();
			if (e.getParents().isEmpty()) {
				current.setHashId(e.getHashId());
			}
			for (int l = 0; l < op.getChildren().size(); l++) {
				SQLQuery u = new SQLQuery();

				// visited.put(op, current);
				tempResult.setCurrent(u);
				unionNo = l;
				combineOperatorsAndOutputQueriesCentralized(p.getInputPlan(l), tempResult, visited);
				if (memo.getMemoValue(p.getInputPlan(l)).isMaterialised()) {
					u = tempResult.get(tempResult.getLastTable().getAlias());
					if (u == null && useCache) {
						// it's a table from cache
						u = new SQLQuery();
						u.addInputTable(tempResult.getLastTable());
						u.setExistsInCache(true);
						tempResult.add(u);
					}
				} else {
					// visited.put(p.getInputPlan(l), u);
					tempResult.add(u);
				}
				u.setHashId(p.getInputPlan(l).getNode().getHashId());
				if (useCache && u.existsInCache()) {
					String tableInCache = registry.get(u.getHashId()).getName();
					tempResult.remove(u);
					u = new SQLQuery();
					if (op.getChildren().size() == 1) {
						u.addInputTable(new Table(tableInCache, tableInCache));
						u.setExistsInCache(true);
						tempResult.add(u);
						return;
					}
					u.setTemporaryTableName(tableInCache);
					// u.addInputTable(new Table(tableInCache, tableInCache));
				}
				unions.add(u);

			}
			if (op.getChildren().size() > 1) {
				tempResult.add(current);
				current.setUnionqueries(unions);
				if (op.getOpCode() == Node.UNION) {
					current.setUnionAll(false);
				}
				visited.put(k, current);
				current.setHashId(e.getHashId());
				if (memo.getMemoValue(k).isMaterialised()) {
					current.setMaterialised(true);
				}
			} else {
				visited.put(k, unions.get(0));
				unions.get(0).setHashId(p.getInputPlan(0).getNode().getHashId());
				if (memo.getMemoValue(k).isMaterialised()) {
					unions.get(0).setMaterialised(true);
				}
			}

		} else if (op.getOpCode() == Node.SELECT) {
			combineOperatorsAndOutputQueriesCentralized(p.getInputPlan(0), tempResult, visited);
			String inputName = tempResult.getLastTable().getAlias();
			Selection s = (Selection) op.getObject();
			Iterator<Operand> it = s.getOperands().iterator();
			while (it.hasNext()) {
				Operand o = it.next();
				if (o instanceof UnaryWhereCondition) {
					UnaryWhereCondition uwc = (UnaryWhereCondition) o;
					UnaryWhereCondition uwcCloned = null;

					uwcCloned = uwc.clone();

					if (!inputName.equals(current.getTemporaryTableName())) {
						Column c = uwcCloned.getAllColumnRefs().get(0);
					//	if (op.isDescendantOfBaseTable(c.getAlias())) {
							uwcCloned.changeColumn(c, new Column(tempResult.getQueryForBaseTable(c.getAlias()),
									c.getName(), c.getAlias()));
							// addOutputIfNotExists(c, tempResult);
					//	}
					}
					current.addUnaryWhereCondition(uwcCloned);
				} else {
					NonUnaryWhereCondition nuwc = (NonUnaryWhereCondition) o;
					NonUnaryWhereCondition nuwcCloned = nuwc.clone();
					nuwcCloned.addRangeFilters(nuwc);
					if (!inputName.equals(current.getTemporaryTableName())) {
						for (Column c : nuwcCloned.getAllColumnRefs()) {
							//if (op.isDescendantOfBaseTable(c.getAlias())) {
								nuwcCloned.changeColumn(c, new Column(tempResult.getQueryForBaseTable(c.getAlias()),
										c.getName(), c.getAlias()));
								// addOutputIfNotExists(c, tempResult);
						//	}
						}
					}
					current.addBinaryWhereCondition(nuwcCloned);
				}
			}

			// visited.put(p.getInputPlan(j), q2);
			if (tempResult.getLastTable() != null) {
				inputName = tempResult.getLastTable().getAlias();
				if (inputName != current.getTemporaryTableName()
						&& !current.getInputTables().contains(tempResult.getLastTable())) {
					current.addInputTable(tempResult.getLastTable());
					addOutputs(current, tempResult.getLastTable().getAlias(), tempResult);
					for (Output o : current.getOutputs()) {
						for (Column c : o.getObject().getAllColumnRefs()) {
							{
								if (op.isDescendantOfBaseTable(c.getAlias())) {
									o.getObject().changeColumn(c, new Column(
											tempResult.getQueryForBaseTable(c.getAlias()), c.getName(), c.getAlias()));
									// addOutputIfNotExists(c, tempResult);
								}
							}
						}

					}
				}

			}

		} else if (op.getOpCode() == Node.LIMIT) {
			current.setLimit(((Integer) op.getObject()).intValue());
			current.setHashId(p.getInputPlan(0).getNode().getHashId());
			combineOperatorsAndOutputQueriesCentralized(p.getInputPlan(0), tempResult, visited);
			if (current.getInputTables().isEmpty()) {
				// limit of a query that exists in the cache
				current.addInputTable(tempResult.getLastTable());
			}
			if (tempResult.isEmpty() && e == this.root) {
				// final limit of a table tha exists in cache
				tempResult.add(current);
			}
		} else if (op.getOpCode() == Node.NESTED) {
			// nested is always materialized
			// current.setNested(true);
			combineOperatorsAndOutputQueriesCentralized(p.getInputPlan(0), tempResult, visited);

		}

		else if (op.getOpCode() == Node.LEFTJOIN || op.getOpCode() == Node.JOINKEY) {
			Operand leftJoinOp = (Operand) op.getObject();
			current.addJoinOperand(leftJoinOp.clone());
			current.setJoinType("left outer join");
			if (op.getOpCode() == Node.JOINKEY) {
				current.setJoinType("join");
			}
			List<String> inputNames = new ArrayList<String>();
			for (int j = 0; j < op.getChildren().size(); j++) {

				combineOperatorsAndOutputQueriesCentralized(p.getInputPlan(j), tempResult, visited);
				inputNames.add(tempResult.getLastTable().getAlias());
				if (tempResult.getLastTable().getAlias() != current.getTemporaryTableName()
						&& !current.getInputTables().contains(tempResult.getLastTable())) {
					current.addInputTable(tempResult.getLastTable());
					addOutputs(current, tempResult.getLastTable().getAlias(), tempResult);
					for (Output o : current.getOutputs()) {
						for (Column c : o.getObject().getAllColumnRefs()) {
							{
								if (op.getChildAt(j).isDescendantOfBaseTable(c.getAlias())) {
									o.getObject().changeColumn(c, new Column(
											tempResult.getQueryForBaseTable(c.getAlias()), c.getName(), c.getAlias()));
									// addOutputIfNotExists(c, tempResult);
								}
							}
						}

					}
				}
			}

			for (Operand o : current.getJoinOperands()) {
				for (Column c : o.getAllColumnRefs()) {
					o.changeColumn(c,
							new Column(tempResult.getQueryForBaseTable(c.getAlias()), c.getName(), c.getAlias()));
				}
			}
			// }

			
		} else if (op.getOpCode() == Node.ORDERBY) {
			combineOperatorsAndOutputQueriesCentralized(p.getInputPlan(0), tempResult, visited);
		
			List<ColumnOrderBy> orderCols = (ArrayList<ColumnOrderBy>) op.getObject();
			current.setOrderBy(orderCols);
			for(Column c:orderCols){
				c.setAlias(null);
			}
			if(!tempResult.getLastTable().getAlias().equals(current.getTemporaryTableName())){
				current.addInputTableIfNotExists(tempResult.getLastTable());
			}

		} else if (op.getOpCode() == Node.GROUPBY) {
			combineOperatorsAndOutputQueriesCentralized(p.getInputPlan(0), tempResult, visited);
			
			List<Column> groupCols = (ArrayList<Column>) op.getObject();
			current.setGroupBy(groupCols);
			//for(Column c:groupCols){
			//	c.setAlias(null);
			//}
			if(!tempResult.getLastTable().getAlias().equals(current.getTemporaryTableName())){
				current.addInputTableIfNotExists(tempResult.getLastTable());
			}

		} else {
			log.error("Unknown Operator in DAG");
		}
		current.setExistsInCache(false);
		if (memo.getMemoValue(k).isMaterialised()) {
			// if (!current.existsInCache()) {
			tempResult.add(current);
			tempResult.setLastTable(current);
			// }
			current.setHashId(e.getHashId());
			log.debug("cardinality estimation for "+current.getTemporaryTableName()+":");
			if(e.getNodeInfo()!=null){
				log.debug(e.getNodeInfo().getNumberOfTuples());
			}
			for (String alias : e.getDescendantBaseTables()) {
				tempResult.trackBaseTableFromQuery(alias, tempResult.getLastTable().getAlias());
			}
			tempResult.setCurrent(old);
		}

	}

	private Operand addFilterJoin(Column filterJoin, Operand previous, ResultList tempResult) {
		if (filterJoin != null) {
			Column filterRenamed = new Column(tempResult.getQueryForBaseTable(filterJoin.getAlias()),
					filterJoin.getName(), filterJoin.getAlias());
			Column toAddToSip = null;
			for (NonUnaryWhereCondition join : tempResult.getCurrent().getBinaryWhereConditions()) {
				if (join.getLeftOp() instanceof Column && join.getRightOp() instanceof Column) {
					if (join.getLeftOp().equals(filterRenamed)) {
						toAddToSip = (Column) join.getRightOp();
						break;
					}
					if (join.getRightOp().equals(filterRenamed)) {
						toAddToSip = (Column) join.getLeftOp();
						break;
					}
				}
			}
			if (toAddToSip == null) {
				System.out.println("other col null!?");
			} else {
				BinaryOperand bo = new BinaryOperand();
				bo.setOperator("||");
				bo.setLeftOp(previous);
				bo.setRightOp(toAddToSip);
				return bo;
			}
		}
		return previous;
	}

	private void addOutputs(SQLQuery current, String inputName, ResultList list) {
		SQLQuery q = list.get(inputName);
		// log.debug("adding Output to:" + current.getTemporaryTableName());

		if (q != null) {
			// log.debug("from q:" + q.getTemporaryTableName());
			for (Output o : q.getOutputs()) {
				// log.debug("out alias:" + o.getOutputName());
				if (!current.getOutputAliases().contains(o.getOutputName())) {
					current.addOutput(inputName, o.getOutputName());
				}
			}
		} else {
			for (madgik.exareme.common.schema.Table table : registry.values()) {
				if (table.getName().equals(inputName)) {
					String schema = table.getSqlDefinition();
					log.debug("table in cache: " + schema);
					if (schema.contains("(")) {
						schema = schema.substring(schema.indexOf("(") + 1);
					}
					schema = schema.replace(")", "");
					schema = schema.replaceAll(",", " ");
					schema = schema.replaceAll("\n", " ");
					String[] outputs = schema.split(" ");
					for (int m = 0; m < outputs.length; m++) {
						String output = outputs[m];
						if (!output.equals(" ") && !output.isEmpty() && !output.toUpperCase().equals("INTEGER")
								&& !output.toUpperCase().equals("INT") && !output.toUpperCase().equals("NUM")
								&& !output.toUpperCase().equals("REAL") && !output.toUpperCase().equals("BLOB")
								&& !output.toUpperCase().equals("NULL") && !output.toUpperCase().equals("NUMERIC")
								&& !output.toUpperCase().equals("TEXT")) {
							if (!current.getOutputAliases().contains(output)) {
								current.addOutput(inputName, output);
							}
						}
					}
				}

			}
		}
	}

	private void combineOperatorsAndOutputQueriesPush(MemoKey k, ResultList tempResult,
			HashMap<MemoKey, SQLQuery> visited, boolean pushToEndpoint) throws CloneNotSupportedException {

		SQLQuery current = tempResult.getCurrent();

		MemoValue v = memo.getMemoValue(k);
		SinglePlan p = v.getPlan();
		Node e = k.getNode();
		SQLQuery old = current;
		if (visited.containsKey(k) && visited.get(k).isMaterialised()) {
			tempResult.setLastTable(visited.get(k));

			tempResult.remove(current);
			for (String alias : e.getDescendantBaseTables()) {
				tempResult.trackBaseTableFromQuery(alias, tempResult.getLastTable().getAlias());
			}
			return;
		}
		boolean toPushChildrenToEndpoint = pushToEndpoint;
		if (!pushToEndpoint && canPushToEndpoint(v, e, visited)) {
			toPushChildrenToEndpoint = true;
			if (!(e.getFirstParent() != null && e.getFirstParent().getOpCode() == Node.NESTED)) {
				v.setMaterialized(true);
				SQLQuery q2 = new SQLQuery();
				q2.setHashId(e.getHashId());
				tempResult.setCurrent(q2);
				current = q2;
				current.setMaterialised(true);
				
			}
			// visited.put(k, current);
		}

		if (useCache && registry.containsKey(e.getHashId()) && e.getHashId() != null) {
			madgik.exareme.common.schema.Table t=registry.get(e.getHashId());
			SQLQuery dummy=new SQLQuery();
			String tableInRegistry = t.getName();
			List<Output> out=Util.getOutputForTable(t.getSqlDefinition(), tableInRegistry);
			Table r = new Table(tableInRegistry, tableInRegistry);
			dummy.addInputTable(r);
			dummy.setOutputs(out);
			dummy.setOutputColumnsDistinct(true);
			tempResult.add(dummy);
			log.debug("adding dummy query:"+dummy.getTemporaryTableName()+" for table in registry:"+tableInRegistry);
			//tempResult.setLastTable();
			// tempResult.trackBaseTableFromQuery(t.getAlias(), t.getAlias());
			//SQLQuery cached = new SQLQuery();
			//cached.setTemporaryTableName(tableInRegistry);
			visited.put(k, dummy);
			dummy.setHashId(e.getHashId());
			// current.setTemporaryTableName(tableInRegistry);
			current.setExistsInCache(true);

			for (String alias : e.getDescendantBaseTables()) {
				tempResult.trackBaseTableFromQuery(alias, dummy.getTemporaryTableName());
			}

			return;
		}

		if (!e.getObject().toString().startsWith("table")) {
			Table t = (Table) k.getNode().getObject();
			tempResult.setLastTable(t);
			// visited.put(k, current);

			// if (memo.getMemoValue(k).isMaterialised()) {
			// current.setMaterialised(true);
			// current.setHashId(e.getHashId());
			// }
			tempResult.trackBaseTableFromQuery(t.getAlias(), t.getAlias());
			return;
		}
		Column repBefore = null;
		if (p.getRepartitionBeforeOp() != null) {
			repBefore = new Column(null, p.getRepartitionBeforeOp().getName(), p.getRepartitionBeforeOp().getAlias());
		}
		Node op = e.getChildAt(p.getChoice());

		if (memo.getMemoValue(k).isMaterialised()) {
			if (!toPushChildrenToEndpoint) {
				SQLQuery q2 = new SQLQuery();
				q2.setHashId(e.getHashId());
				tempResult.setCurrent(q2);
				current = q2;
				current.setMaterialised(true);
			}
			visited.put(k, current);
		}

		if (repBefore != null) {
			if (!(pushToEndpoint && current.getRepartition() != null)) {
				current.setRepartition(repBefore, partitionNo);
				current.setPartition(repBefore);
			}
		}

		if (op.getOpCode() == Node.PROJECT || op.getOpCode() == Node.BASEPROJECT) {
			String inputName;

			Projection prj = (Projection) op.getObject();
			// current = q2;
			// q2.setPartition(repAfter, partitionNo);

			for (Output o : prj.getOperands()) {
				current.getOutputs().add(o.clone());
			}
			current.setOutputColumnsDinstict(prj.isDistinct());
			// current.getOutputs().addAll(prj.getOperands());

			combineOperatorsAndOutputQueriesPush(p.getInputPlan(0), tempResult, visited, toPushChildrenToEndpoint);
			// visited.put(p.getInputPlan(j), q2);

			if (op.getOpCode() == Node.PROJECT) {
				// if we have a final result, remove previous projections
				for (int l = 0; l < current.getOutputs().size(); l++) {
					Output o = current.getOutputs().get(l);
					boolean exists = false;
					for (Output pr : prj.getOperands()) {
						if (pr.getOutputName().equals(o.getOutputName())) {
							exists = true;
							break;
						}
					}
					if (!exists) {
						current.getOutputs().remove(o);
						l--;
					}
				}
			}
			if (tempResult.getLastTable() != null) {
				inputName = tempResult.getLastTable().getAlias();
				if (inputName != current.getTemporaryTableName()
						&& !current.getInputTables().contains(tempResult.getLastTable())) {
					current.addInputTable(tempResult.getLastTable());
					for (Output o : current.getOutputs()) {
						for (Column c : o.getObject().getAllColumnRefs()) {
							{
								if (op.isDescendantOfBaseTable(c.getAlias())) {
									o.getObject().changeColumn(c, new Column(
											tempResult.getQueryForBaseTable(c.getAlias()), c.getName(), c.getAlias()));
								}
							}
						}

					}
				}

			}

		} else if (op.getOpCode() == Node.JOIN) {
			NonUnaryWhereCondition nuwc = (NonUnaryWhereCondition) op.getObject();
			NonUnaryWhereCondition nuwcCloned = nuwc.clone();
			nuwcCloned.addRangeFilters(nuwc);
			current.addBinaryWhereCondition(nuwcCloned);
			List<String> inputNames = new ArrayList<String>();
			for (int j = 0; j < op.getChildren().size(); j++) {

				combineOperatorsAndOutputQueriesPush(p.getInputPlan(j), tempResult, visited, toPushChildrenToEndpoint);
				inputNames.add(tempResult.getLastTable().getAlias());
				if (tempResult.getLastTable().getAlias() != current.getTemporaryTableName()
						&& !current.getInputTables().contains(tempResult.getLastTable())) {
					current.addInputTable(tempResult.getLastTable());
					addOutputs(current, tempResult.getLastTable().getAlias(), tempResult);
					for (Output o : current.getOutputs()) {
						for (Column c : o.getObject().getAllColumnRefs()) {
							{
								if (op.getChildAt(j).isDescendantOfBaseTable(c.getAlias())) {
									o.getObject().changeColumn(c, new Column(
											tempResult.getQueryForBaseTable(c.getAlias()), c.getName(), c.getAlias()));
								}
							}
						}

					}
				}
			}

			for (NonUnaryWhereCondition bwc : current.getBinaryWhereConditions()) {

				for (int j = 0; j < op.getChildren().size(); j++) {
					for (Column c : bwc.getAllColumnRefs()) {
						// for (Operand o : bwc.getOperands()) {

						// List<Column> cs = o.getAllColumnRefs();
						// if (!cs.isEmpty()) {
						// not constant
						// Column c = cs.get(0);
						if (op.getChildAt(j).isDescendantOfBaseTable(c.getAlias())) {
							bwc.changeColumn(c, new Column(tempResult.getQueryForBaseTable(c.getAlias()), c.getName(),
									c.getAlias()));
							// }
						}

					}
				}
			}
			// }

			/*
			 * for (Output o : current.getOutputs()) { for (Column c :
			 * o.getObject().getAllColumnRefs()) { { for (int j = 0; j <
			 * op.getChildren().size(); j++) { Node child = op.getChildAt(j); if
			 * (child.isDescendantOfBaseTable(c.getAlias())) {
			 * o.getObject().changeColumn(c, new Column(inputNames.get(j),
			 * c.getName())); } } } // o.getObject().changeColumn(c, c); } }
			 */
			// tempResult.add(current);
		} else if (op.getOpCode() == Node.UNION || op.getOpCode() == Node.UNIONALL) {
			List<SQLQuery> unions = new ArrayList<SQLQuery>();
			if (e.getParents().isEmpty()) {
				current.setHashId(e.getHashId());
			}
			boolean existsPartitioned = false;
			boolean existsNonPartitioned = false;
			for (int l = 0; l < op.getChildren().size(); l++) {
				SQLQuery u = new SQLQuery();

				// visited.put(op, current);
				tempResult.setCurrent(u);
				combineOperatorsAndOutputQueriesPush(p.getInputPlan(l), tempResult, visited, false);
				// visited.put(p.getInputPlan(l), u);
				if (memo.getMemoValue(p.getInputPlan(l)).isMaterialised()) {
					u = tempResult.get(tempResult.getLastTable().getAlias());
					if (u == null && useCache) {
						// it's a table from cache
						u = new SQLQuery();
						u.addInputTable(tempResult.getLastTable());
						u.setExistsInCache(true);
						tempResult.add(u);
					}
				} else {
					// visited.put(p.getInputPlan(l), u);
					tempResult.add(u);
				}

				u.setHashId(p.getInputPlan(l).getNode().getHashId());
				if (useCache && u.existsInCache()) {
					String tableInCache = registry.get(u.getHashId()).getName();
					tempResult.remove(u);
					u = new SQLQuery();
					if (op.getChildren().size() == 1) {
						u.addInputTable(new Table(tableInCache, tableInCache));
						u.setExistsInCache(true);
						tempResult.add(u);
						return;
					}
					u.setTemporaryTableName(tableInCache);
					// u.addInputTable(new Table(tableInCache, tableInCache));
				}
				u.setRepartition(null);
				unions.add(u);
				
			}
			if (unions.size() > 1) {
				tempResult.add(current);
				current.setUnionqueries(unions);
				if (op.getOpCode() == Node.UNION) {
					current.setUnionAll(false);
				}
				visited.put(k, current);
				current.setHashId(e.getHashId());
				if (memo.getMemoValue(k).isMaterialised()) {
					current.setMaterialised(true);
				}
				if (op.getOpCode() == Node.UNION) {
					// make all unions have the same number of partitions
					SQLQuery first = unions.get(0);
					String ptCol = first.getOutputs().get(0).getOutputName();
					for (Output a : first.getOutputs()) {
						if (!a.getObject().getAllColumnRefs().isEmpty()) {
							ptCol = a.getOutputName();
							break;
						}
					}

					Column pt = new Column(ptCol, ptCol);
					first.setRepartition(pt, partitionNo);
					first.setPartition(pt);
					for (int un = 1; un < unions.size(); un++) {
						SQLQuery u = unions.get(un);
						u.setRepartition(pt, partitionNo);
						u.setPartition(pt);
					}
					
				}
			} else {
				visited.put(k, unions.get(0));
				unions.get(0).setHashId(p.getInputPlan(0).getNode().getHashId());
				if (memo.getMemoValue(k).isMaterialised()) {
					unions.get(0).setMaterialised(true);
				}
			}
		} else if (op.getOpCode() == Node.SELECT) {
			combineOperatorsAndOutputQueriesPush(p.getInputPlan(0), tempResult, visited, toPushChildrenToEndpoint);
			String inputName = tempResult.getLastTable().getAlias();
			Selection s = (Selection) op.getObject();
			Iterator<Operand> it = s.getOperands().iterator();
			while (it.hasNext()) {
				Operand o = it.next();
				if (o instanceof UnaryWhereCondition) {
					UnaryWhereCondition uwc = (UnaryWhereCondition) o;
					UnaryWhereCondition uwcCloned = null;
					uwcCloned = uwc.clone();
					if (!inputName.equals(current.getTemporaryTableName())) {
						Column c = uwcCloned.getAllColumnRefs().get(0);
						uwcCloned.changeColumn(c,
								new Column(tempResult.getQueryForBaseTable(c.getAlias()), c.getName(), c.getAlias()));
					}
					current.addUnaryWhereCondition(uwcCloned);
				} else {
					NonUnaryWhereCondition nuwc = (NonUnaryWhereCondition) o;
					NonUnaryWhereCondition nuwcCloned = nuwc.clone();
					nuwcCloned.addRangeFilters(nuwc);
					if (!inputName.equals(current.getTemporaryTableName())) {
						for (Column c : nuwcCloned.getAllColumnRefs()) {
							nuwcCloned.changeColumn(c, new Column(tempResult.getQueryForBaseTable(c.getAlias()),
									c.getName(), c.getAlias()));
						}
					}
					current.addBinaryWhereCondition(nuwcCloned);
				}
			}

			// visited.put(p.getInputPlan(j), q2);
			if (tempResult.getLastTable() != null) {
				inputName = tempResult.getLastTable().getAlias();
				if (inputName != current.getTemporaryTableName()
						&& !current.getInputTables().contains(tempResult.getLastTable())) {
					current.addInputTable(tempResult.getLastTable());
					addOutputs(current, tempResult.getLastTable().getAlias(), tempResult);
					for (Output o : current.getOutputs()) {
						for (Column c : o.getObject().getAllColumnRefs()) {
							{
								if (op.isDescendantOfBaseTable(c.getAlias())) {
									o.getObject().changeColumn(c, new Column(
											tempResult.getQueryForBaseTable(c.getAlias()), c.getName(), c.getAlias()));
								}
							}
						}

					}
				}

			}

		} else if (op.getOpCode() == Node.LIMIT) {
			current.setLimit(((Integer) op.getObject()).intValue());
			current.setHashId(p.getInputPlan(0).getNode().getHashId());
			combineOperatorsAndOutputQueriesPush(p.getInputPlan(0), tempResult, visited, toPushChildrenToEndpoint);
			if (current.getInputTables().isEmpty()) {
				// limit of a query that exists in the cache
				current.addInputTable(tempResult.getLastTable());
			}
			if (tempResult.isEmpty() && e == this.root) {
				// final limit of a table tha exists in cache
				tempResult.add(current);
			}
		} else if (op.getOpCode() == Node.LEFTJOIN || op.getOpCode() == Node.JOINKEY) {
			Operand leftJoinOp = (Operand) op.getObject();
			current.addJoinOperand(leftJoinOp);
			current.setJoinType("left outer join");
			if (op.getOpCode() == Node.JOINKEY) {
				current.setJoinType("join");
			}
			List<String> inputNames = new ArrayList<String>();
			for (int j = 0; j < op.getChildren().size(); j++) {

				combineOperatorsAndOutputQueriesPush(p.getInputPlan(j), tempResult, visited, toPushChildrenToEndpoint);
				inputNames.add(tempResult.getLastTable().getAlias());
				if (tempResult.getLastTable().getAlias() != current.getTemporaryTableName()
						&& !current.getInputTables().contains(tempResult.getLastTable())) {
					current.addInputTable(tempResult.getLastTable());
					addOutputs(current, tempResult.getLastTable().getAlias(), tempResult);
					for (Output o : current.getOutputs()) {
						for (Column c : o.getObject().getAllColumnRefs()) {
							{
								if (op.getChildAt(j).isDescendantOfBaseTable(c.getAlias())) {
									o.getObject().changeColumn(c, new Column(
											tempResult.getQueryForBaseTable(c.getAlias()), c.getName(), c.getAlias()));
								}
							}
						}

					}
				}
			}

			for (Operand o : current.getJoinOperands()) {
				for (Column c : o.getAllColumnRefs()) {
					o.changeColumn(c,
							new Column(tempResult.getQueryForBaseTable(c.getAlias()), c.getName(), c.getAlias()));
				}
			}
		} else if (op.getOpCode() == Node.NESTED) {
			// nested is always materialized
			// current.setNested(true);
			// tempResult.trackBaseTableFromQuery(op.getDescendantBaseTables().iterator().next(),
			// current.getTemporaryTableName());
			combineOperatorsAndOutputQueriesPush(p.getInputPlan(0), tempResult, visited, toPushChildrenToEndpoint);
			current.setRepartition(repBefore, partitionNo);
			current.setPartition(repBefore);
		} else if (op.getOpCode() == Node.ORDERBY) {
			combineOperatorsAndOutputQueries(p.getInputPlan(0), tempResult, visited);
			List<ColumnOrderBy> orderCols = (ArrayList<ColumnOrderBy>) op.getObject();
			current.setOrderBy(orderCols);
			for(Column c:orderCols){
				c.setAlias(null);
			}
			if(!tempResult.getLastTable().getAlias().equals(current.getTemporaryTableName())){
				current.addInputTableIfNotExists(tempResult.getLastTable());
			}
		} else if (op.getOpCode() == Node.GROUPBY) {
			combineOperatorsAndOutputQueries(p.getInputPlan(0), tempResult, visited);
			
			List<Column> groupCols = (ArrayList<Column>) op.getObject();
			current.setGroupBy(groupCols);
			//for(Column c:groupCols){
			//	c.setAlias(null);
			//}
			if(!tempResult.getLastTable().getAlias().equals(current.getTemporaryTableName())){
				current.addInputTableIfNotExists(tempResult.getLastTable());
			}

		} else {
			log.error("Unknown Operator in DAG");
		}
		current.setExistsInCache(false);
		if (memo.getMemoValue(k).isMaterialised() && !pushToEndpoint) {
			tempResult.add(current);
			tempResult.setLastTable(current);
			current.setHashId(e.getHashId());
			for (String alias : e.getDescendantBaseTables()) {
				tempResult.trackBaseTableFromQuery(alias, tempResult.getLastTable().getAlias());
			}
			tempResult.setCurrent(old);
		}
		if (old != current && old.getRepartition() == null) {
			old.setPartition(current.getPartitionColumn());
		}

	}

	private boolean canPushToEndpoint(MemoValue v, Node e, HashMap<MemoKey, SQLQuery> visited) {
		if (e.getDescendantBaseTables().isEmpty()) {
			return false;
		}
		String dbID = null;
		for (String s : e.getDescendantBaseTables()) {
			Table tab = new Table(n2a.getOriginalName(s), s);
			String tableEndpoint = tab.getDBName();
			if (tableEndpoint == null) {
				return false;
			}
			if (dbID == null) {
				dbID = tableEndpoint;
			} else {
				if (!DBInfoReaderDB.isOnSameDB(dbID, tableEndpoint)) {
					return false;
				}
			}
		}
		if (e.getDescendantBaseTables().size() == 1) {
			return true;
		}
		return doesNotContainMoreUsedInput(v, visited, v.getUsed());
	}

	private boolean doesNotContainMoreUsedInput(MemoValue v, HashMap<MemoKey, SQLQuery> visited, int times) {
		
		for (int i = 0; i < v.getPlan().noOfInputPlans(); i++) {
			MemoValue inputV =  memo.getMemoValue(v.getPlan().getInputPlan(i));
			if (inputV.getUsed()>times || visited.containsKey(v.getPlan().getInputPlan(i))) {
				return false;
			}
			if (!doesNotContainMoreUsedInput(inputV, visited, times)) {
				return false;
			}
		}
		return true;
	}

	public void setN2a(NamesToAliases n2a) {
		this.n2a = n2a;
	}

	public void setUseSIP(boolean useSIP) {
		this.useSIP = useSIP;
	}

	public void setSipStruct(SipStructure sipStruct) {
		this.sipStruct = sipStruct;
	}

	public void setSipToUnions(SipToUnions sipToUnions) {
		this.sipToUnions = sipToUnions;

	}
	
	private void combineOperatorsAndOutputQueriesCentralizedPush(MemoKey k, ResultList tempResult,
			HashMap<MemoKey, SQLQuery> visited, boolean pushToEndpoint) throws CloneNotSupportedException {

		SQLQuery current = tempResult.getCurrent();
		MemoValue v = memo.getMemoValue(k);

		SinglePlan p = v.getPlan();
		Node e = k.getNode();

		boolean toPushChildrenToEndpoint = pushToEndpoint;
		if (!pushToEndpoint && canPushToEndpoint(v, e, visited)) {
			toPushChildrenToEndpoint = true;
			if (!(e.getFirstParent() != null && e.getFirstParent().getOpCode() == Node.NESTED)) {
				v.setMaterialized(true);
				SQLQuery q2 = new SQLQuery();
				q2.setHashId(e.getHashId());
				tempResult.setCurrent(q2);
				current = q2;
				current.setMaterialised(true);
				
			}
			// visited.put(k, current);
		}

		
		if (useCache && registry.containsKey(e.getHashId()) && e.getHashId() != null) {
			String tableInRegistry = registry.get(e.getHashId()).getName();
			Table t = new Table(tableInRegistry, tableInRegistry);
			tempResult.setLastTable(t);
			SQLQuery cached = new SQLQuery();
			cached.setTemporaryTableName(tableInRegistry);
			visited.put(k, cached);
			cached.setHashId(e.getHashId());
			current.setExistsInCache(true);

			for (String alias : e.getDescendantBaseTables()) {
				tempResult.trackBaseTableFromQuery(alias, tableInRegistry);
			}

			return;
		}

		if (visited.containsKey(k) && visited.get(k).isMaterialised()) {
			tempResult.setLastTable(visited.get(k));
			tempResult.remove(current);
			for (String alias : e.getDescendantBaseTables()) {
				tempResult.trackBaseTableFromQuery(alias, tempResult.getLastTable().getAlias());
			}
			return;
		}

		if (!e.getObject().toString().startsWith("table")) {
			Table t = (Table) k.getNode().getObject();
			tempResult.setLastTable(t);
			if (t.isFederated()) {
				// visited.put(k, current);
				if (memo.getMemoValue(k).isMaterialised()) {
					current.setMaterialised(true);
					current.setHashId(e.getHashId());
				}
			}

			tempResult.trackBaseTableFromQuery(t.getAlias(), t.getAlias());

			return;
		}

		Node op = e.getChildAt(p.getChoice());
		SQLQuery old = current;
		if (memo.getMemoValue(k).isMaterialised()) {
			if (!toPushChildrenToEndpoint) {
			SQLQuery q2 = new SQLQuery();
			q2.setHashId(e.getHashId());

			tempResult.setCurrent(q2);
			current = q2;
			current.setMaterialised(true);
			}
			visited.put(k, current);
		}

		if (op.getOpCode() == Node.PROJECT || op.getOpCode() == Node.BASEPROJECT) {
			String inputName;

			Projection prj = (Projection) op.getObject();
	
			for (Output o : prj.getOperands()) {
				current.getOutputs().add(o.clone());
			}
			current.setOutputColumnsDinstict(prj.isDistinct());

			combineOperatorsAndOutputQueriesCentralizedPush(p.getInputPlan(0), tempResult, visited, toPushChildrenToEndpoint);

			if (op.getOpCode() == Node.PROJECT) {
				Table t=(Table)e.getObject();
				current.setTemporaryTableName(t.getName());
				for (int l = 0; l < current.getOutputs().size(); l++) {
					Output o = current.getOutputs().get(l);
					boolean exists = false;
					for (Output pr : prj.getOperands()) {
						if (pr.getOutputName().equals(o.getOutputName())) {
							exists = true;
							break;
						}
					}
					if (!exists) {
						current.getOutputs().remove(o);
						l--;
					}
				}

				

			}
			
			if (tempResult.getLastTable() != null) {
				inputName = tempResult.getLastTable().getAlias();
				if (inputName != current.getTemporaryTableName()
						&& !current.getInputTables().contains(tempResult.getLastTable())) {
					current.addInputTableIfNotExists(tempResult.getLastTable());

					for (Output o : current.getOutputs()) {
						for (Column c : o.getObject().getAllColumnRefs()) {
							{
								if (op.isDescendantOfBaseTable(c.getAlias())) {
									o.getObject().changeColumn(c, new Column(
											tempResult.getQueryForBaseTable(c.getAlias()), c.getName(), c.getAlias()));
								}
							}
						}

					}
				}

			}

		} else if (op.getOpCode() == Node.JOIN) {
			NonUnaryWhereCondition nuwc = (NonUnaryWhereCondition) op.getObject();
			NonUnaryWhereCondition nuwcCloned = nuwc.clone();
			nuwcCloned.addRangeFilters(nuwc);
			current.addBinaryWhereCondition(nuwcCloned);
			List<String> inputNames = new ArrayList<String>();
			for (int j = 0; j < op.getChildren().size(); j++) {

				combineOperatorsAndOutputQueriesCentralizedPush(p.getInputPlan(j), tempResult, visited, toPushChildrenToEndpoint);
				String lastRes = tempResult.getLastTable().getAlias();
				inputNames.add(lastRes);
				if (lastRes != current.getTemporaryTableName()
						&& !current.getInputTables().contains(tempResult.getLastTable())) {
					current.addInputTableIfNotExists(tempResult.getLastTable());
					addOutputs(current, lastRes, tempResult);
					if (addIndicesToMatQueries && p.getInputPlan(j).getNode().isMaterialised()) {
						if (!matResultUsedCols.containsKey(lastRes)) {
							matResultUsedCols.put(lastRes, new HashSet<Column>());
						}
						Set<Column> usedCols = matResultUsedCols.get(lastRes);
						usedCols.add(nuwc.getOp(j).getAllColumnRefs().get(0));
					}
					for (Output o : current.getOutputs()) {
						for (Column c : o.getObject().getAllColumnRefs()) {
							{
								if (op.getChildAt(j).isDescendantOfBaseTable(c.getAlias())) {
									o.getObject().changeColumn(c, new Column(
											tempResult.getQueryForBaseTable(c.getAlias()), c.getName(), c.getAlias()));
									// addOutputIfNotExists(c, tempResult);
								}
							}
						}

					}
				}
			}

			for (NonUnaryWhereCondition bwc : current.getBinaryWhereConditions()) {

				for (int j = 0; j < op.getChildren().size(); j++) {
				//	for (Operand o : bwc.getOperands()) {
						for (Column c : bwc.getAllColumnRefs()) {
					//	List<Column> cs = o.getAllColumnRefs();
					//	if (!cs.isEmpty()) {
							// not constant
						//	Column c = cs.get(0);

							if (op.getChildAt(j).isDescendantOfBaseTable(c.getAlias())) {
								bwc.changeColumn(c, new Column(tempResult.getQueryForBaseTable(c.getAlias()),
										c.getName(), c.getAlias()));
								// addOutputIfNotExists(c, tempResult);
							}
						}

					}
				}
			

		} else if (op.getOpCode() == Node.UNION || op.getOpCode() == Node.UNIONALL) {
			List<SQLQuery> unions = new ArrayList<SQLQuery>();
			if (e.getParents().isEmpty()) {
				current.setHashId(e.getHashId());
			}
			for (int l = 0; l < op.getChildren().size(); l++) {
				SQLQuery u = new SQLQuery();

				// visited.put(op, current);
				tempResult.setCurrent(u);
				unionNo = l;
				combineOperatorsAndOutputQueriesCentralizedPush(p.getInputPlan(l), tempResult, visited, toPushChildrenToEndpoint);
				if (memo.getMemoValue(p.getInputPlan(l)).isMaterialised()) {
					u = tempResult.get(tempResult.getLastTable().getAlias());
					if (u == null && useCache) {
						// it's a table from cache
						u = new SQLQuery();
						u.addInputTable(tempResult.getLastTable());
						u.setExistsInCache(true);
						tempResult.add(u);
					}
				} else {
					// visited.put(p.getInputPlan(l), u);
					tempResult.add(u);
				}
				u.setHashId(p.getInputPlan(l).getNode().getHashId());
				if (useCache && u.existsInCache()) {
					String tableInCache = registry.get(u.getHashId()).getName();
					tempResult.remove(u);
					u = new SQLQuery();
					if (op.getChildren().size() == 1) {
						u.addInputTable(new Table(tableInCache, tableInCache));
						u.setExistsInCache(true);
						tempResult.add(u);
						return;
					}
					u.setTemporaryTableName(tableInCache);
					// u.addInputTable(new Table(tableInCache, tableInCache));
				}
				unions.add(u);

			}
			if (op.getChildren().size() > 1) {
				tempResult.add(current);
				current.setUnionqueries(unions);
				if (op.getOpCode() == Node.UNION) {
					current.setUnionAll(false);
				}
				visited.put(k, current);
				current.setHashId(e.getHashId());
				if (memo.getMemoValue(k).isMaterialised()) {
					current.setMaterialised(true);
				}
			} else {
				visited.put(k, unions.get(0));
				unions.get(0).setHashId(p.getInputPlan(0).getNode().getHashId());
				if (memo.getMemoValue(k).isMaterialised()) {
					unions.get(0).setMaterialised(true);
				}
			}

		} else if (op.getOpCode() == Node.SELECT) {
			combineOperatorsAndOutputQueriesCentralizedPush(p.getInputPlan(0), tempResult, visited, toPushChildrenToEndpoint);
			String inputName = tempResult.getLastTable().getAlias();
			Selection s = (Selection) op.getObject();
			Iterator<Operand> it = s.getOperands().iterator();
			while (it.hasNext()) {
				Operand o = it.next();
				if (o instanceof UnaryWhereCondition) {
					UnaryWhereCondition uwc = (UnaryWhereCondition) o;
					UnaryWhereCondition uwcCloned = null;

					uwcCloned = uwc.clone();

					if (!inputName.equals(current.getTemporaryTableName())) {
						Column c = uwcCloned.getAllColumnRefs().get(0);
					//	if (op.isDescendantOfBaseTable(c.getAlias())) {
							uwcCloned.changeColumn(c, new Column(tempResult.getQueryForBaseTable(c.getAlias()),
									c.getName(), c.getAlias()));
							// addOutputIfNotExists(c, tempResult);
					//	}
					}
					current.addUnaryWhereCondition(uwcCloned);
				} else {
					NonUnaryWhereCondition nuwc = (NonUnaryWhereCondition) o;
					NonUnaryWhereCondition nuwcCloned = nuwc.clone();
					nuwcCloned.addRangeFilters(nuwc);
					if (!inputName.equals(current.getTemporaryTableName())) {
						for (Column c : nuwcCloned.getAllColumnRefs()) {
							//if (op.isDescendantOfBaseTable(c.getAlias())) {
								nuwcCloned.changeColumn(c, new Column(tempResult.getQueryForBaseTable(c.getAlias()),
										c.getName(), c.getAlias()));
								// addOutputIfNotExists(c, tempResult);
						//	}
						}
					}
					current.addBinaryWhereCondition(nuwcCloned);
				}
			}

			// visited.put(p.getInputPlan(j), q2);
			if (tempResult.getLastTable() != null) {
				inputName = tempResult.getLastTable().getAlias();
				if (inputName != current.getTemporaryTableName()
						&& !current.getInputTables().contains(tempResult.getLastTable())) {
					current.addInputTable(tempResult.getLastTable());
					addOutputs(current, tempResult.getLastTable().getAlias(), tempResult);
					for (Output o : current.getOutputs()) {
						for (Column c : o.getObject().getAllColumnRefs()) {
							{
								if (op.isDescendantOfBaseTable(c.getAlias())) {
									o.getObject().changeColumn(c, new Column(
											tempResult.getQueryForBaseTable(c.getAlias()), c.getName(), c.getAlias()));
									// addOutputIfNotExists(c, tempResult);
								}
							}
						}

					}
				}

			}

		} else if (op.getOpCode() == Node.LIMIT) {
			current.setLimit(((Integer) op.getObject()).intValue());
			current.setHashId(p.getInputPlan(0).getNode().getHashId());
			combineOperatorsAndOutputQueriesCentralizedPush(p.getInputPlan(0), tempResult, visited, toPushChildrenToEndpoint);
			if (current.getInputTables().isEmpty()) {
				// limit of a query that exists in the cache
				current.addInputTable(tempResult.getLastTable());
			}
			if (tempResult.isEmpty() && e == this.root) {
				// final limit of a table tha exists in cache
				tempResult.add(current);
			}
		} else if (op.getOpCode() == Node.NESTED) {
			// nested is always materialized
			// current.setNested(true);
			combineOperatorsAndOutputQueriesCentralizedPush(p.getInputPlan(0), tempResult, visited, toPushChildrenToEndpoint);

		}

		else if (op.getOpCode() == Node.LEFTJOIN || op.getOpCode() == Node.JOINKEY) {
			Operand leftJoinOp = (Operand) op.getObject();
			current.addJoinOperand(leftJoinOp.clone());
			current.setJoinType("left outer join");
			if (op.getOpCode() == Node.JOINKEY) {
				current.setJoinType("join");
			}
			List<String> inputNames = new ArrayList<String>();
			for (int j = 0; j < op.getChildren().size(); j++) {

				combineOperatorsAndOutputQueriesCentralizedPush(p.getInputPlan(j), tempResult, visited, toPushChildrenToEndpoint);
				inputNames.add(tempResult.getLastTable().getAlias());
				if (tempResult.getLastTable().getAlias() != current.getTemporaryTableName()
						&& !current.getInputTables().contains(tempResult.getLastTable())) {
					current.addInputTable(tempResult.getLastTable());
					addOutputs(current, tempResult.getLastTable().getAlias(), tempResult);
					for (Output o : current.getOutputs()) {
						for (Column c : o.getObject().getAllColumnRefs()) {
							{
								if (op.getChildAt(j).isDescendantOfBaseTable(c.getAlias())) {
									o.getObject().changeColumn(c, new Column(
											tempResult.getQueryForBaseTable(c.getAlias()), c.getName(), c.getAlias()));
									// addOutputIfNotExists(c, tempResult);
								}
							}
						}

					}
				}
			}

			for (Operand o : current.getJoinOperands()) {
				for (Column c : o.getAllColumnRefs()) {
					o.changeColumn(c,
							new Column(tempResult.getQueryForBaseTable(c.getAlias()), c.getName(), c.getAlias()));
				}
			}
			// }

			
		} else if (op.getOpCode() == Node.ORDERBY) {
			combineOperatorsAndOutputQueriesCentralizedPush(p.getInputPlan(0), tempResult, visited,toPushChildrenToEndpoint);
		
			List<ColumnOrderBy> orderCols = (ArrayList<ColumnOrderBy>) op.getObject();
			current.setOrderBy(orderCols);
			for(Column c:orderCols){
				c.setAlias(null);
			}
			if(!tempResult.getLastTable().getAlias().equals(current.getTemporaryTableName())){
				current.addInputTableIfNotExists(tempResult.getLastTable());
			}

		} else if (op.getOpCode() == Node.GROUPBY) {
			combineOperatorsAndOutputQueriesCentralizedPush(p.getInputPlan(0), tempResult, visited,toPushChildrenToEndpoint);
			
			List<Column> groupCols = (ArrayList<Column>) op.getObject();
			current.setGroupBy(groupCols);
			//for(Column c:groupCols){
			///	c.setAlias(null);
			//}
			if(!tempResult.getLastTable().getAlias().equals(current.getTemporaryTableName())){
				current.addInputTableIfNotExists(tempResult.getLastTable());
			}

		} else {
			log.error("Unknown Operator in DAG");
		}
		current.setExistsInCache(false);
		if (memo.getMemoValue(k).isMaterialised()&& !pushToEndpoint) {
			// if (!current.existsInCache()) {
			tempResult.add(current);
			tempResult.setLastTable(current);
			// }
			current.setHashId(e.getHashId());
			log.debug("cardinality estimation for "+current.getTemporaryTableName()+":");
			if(e.getNodeInfo()!=null){
				log.debug(e.getNodeInfo().getNumberOfTuples());
			}
			for (String alias : e.getDescendantBaseTables()) {
				tempResult.trackBaseTableFromQuery(alias, tempResult.getLastTable().getAlias());
			}
			tempResult.setCurrent(old);
		}

	}

}

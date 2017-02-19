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

	private static final org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(SinlgePlanDFLGenerator.class);

	SinlgePlanDFLGenerator(Node n, int partNo, Memo m, 
			boolean useCache) {
		this.root = n;
		this.partitionNo = partNo;
		this.memo = m;
		
	}
	
	public SinlgePlanDFLGenerator(Node n, int partNo, Memo m){
		this.root = n;
		this.partitionNo = partNo;
		this.memo = m;
	}

	public ResultList generate() {
		ResultList qs = new ResultList();
		MemoKey rootkey = new MemoKey(root, null);
	memo.getMemoValue(rootkey).setMaterialized(true);

		// printPlan(rootkey);
		qs.setCurrent(new SQLQuery());
		try {

					combineOperatorsAndOutputQueriesCentralized(rootkey, qs, new HashMap<MemoKey, SQLQuery>());
		
		} catch (CloneNotSupportedException clone) {
			log.error("Could not generate plan " + clone.getMessage());
		}
		// for (SQLQuery q : qs) {
		// System.out.println(q.toDistSQL() + "\n");
		// }

		if (qs.isEmpty()) {
			
				log.error("Decomposer produced empty result");
			
		} 


		boolean pushUnions = DecomposerUtils.PUSH_UNIONS;
		if (qs.size() > 1 && pushUnions ) {
			SQLQuery last = qs.get(qs.size() - 1);
			if (last.getOrderBy().isEmpty() && last.getGroupBy().isEmpty() && last.getLimit() < 0) {
				if (last.getUnionqueries().size() > 1) {
					if (last.isUnionAll()) {
						for (SQLQuery u : last.getUnionqueries()) {
							if (u.isFederated()) {
								continue;
							}
							u.setTemporary(false);
							Set<Column> rippedOuts = new HashSet<Column>();
							for (Column c : u.getAllOutputColumns()) {
								rippedOuts.add(c);
							}

							List<Output> newOuts = new ArrayList<Output>();
							for (Column ripped : rippedOuts) {
								newOuts.add(new Output(ripped.getName(), ripped));
								for (Output out : u.getOutputs()) {
									if (out.getObject().equals(ripped)) {
										out.setObject(new Column(null, ripped.getName()));
									} else {
										out.getObject().changeColumn(ripped, new Column(null, ripped.getName()));
									}
								}
							}
							u.setStringOutputs(u.getOutputSQL());
							u.setOutputs(newOuts);
						}
						qs.remove(last);
					} else {
						Map<String, List<SQLQuery>> outputsToQueries=new HashMap<String, List<SQLQuery>>();
						for (SQLQuery u : last.getUnionqueries()) {
							if (u.isFederated()) {
								continue;
							}
							//u.setTemporary(false);
							Set<Column> rippedOuts = new HashSet<Column>();
							for (Column c : u.getAllOutputColumns()) {
								rippedOuts.add(c);
							}

							List<Output> newOuts = new ArrayList<Output>();
							for (Column ripped : rippedOuts) {
								newOuts.add(new Output(ripped.getName(), ripped));
								for (Output out : u.getOutputs()) {
									if (out.getObject().equals(ripped)) {
										out.setObject(new Column(null, ripped.getName()));
									} else {
										out.getObject().changeColumn(ripped, new Column(null, ripped.getName()));
									}
								}
							}
							if(!outputsToQueries.containsKey(u.getOutputSQL())){
								outputsToQueries.put(u.getOutputSQL(), new ArrayList<SQLQuery>());
							}
							outputsToQueries.get(u.getOutputSQL()).add(u);
							//last.setStringOutputs(u.getOutputSQL());
							u.setOutputs(newOuts);
						}
						if(outputsToQueries.size()==1){
							last.setStringOutputs(outputsToQueries.keySet().iterator().next());
						}
						else{
							qs.remove(last);
							for(String o:outputsToQueries.keySet()){
								if(outputsToQueries.get(o).size()>1){
								SQLQuery union=new SQLQuery();
								union.setStringOutputs(o);
								union.setUnionAll(false);
								union.setUnionqueries(outputsToQueries.get(o));
								}
								else{
									SQLQuery out=outputsToQueries.get(o).get(0);
									out.setOutputColumnsDistinct(true);
									out.setStringOutputs(o);
								}
							}
						}
						
					}
				}
			}
		}
 if (DecomposerUtils.REMOVE_OUTPUTS) {
			removeOutputs(qs);
		}

		// remove not needed columns from nested subqueries

		SQLQuery last = qs.get(qs.size() - 1);
		int merge = DecomposerUtils.MERGE_UNIONS;
		while (merge > 0 && last.getUnionqueries().size() > merge) {

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
				if (i % merge == (merge - 1)) {
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
		if (this.partitionNo > 1) {
			for (SQLQuery s : qs) {
				s.setNumberOfPartitions(partitionNo);
			}
		}
		return qs;
	}

	private void removeOutputs(ResultList qs) {
		log.debug("Removing Outputs...");
		if (qs.size() > 1) {
			List<SQLQuery> unions = qs.get(qs.size() - 1).getUnionqueries();
			for (int i = qs.size() - 2; i > -1; i--) {
				// Set<SQLQuery> queriesUsingQ=new HashSet<SQLQuery>();
				SQLQuery q = qs.get(i);
				if (unions.contains(q)||!q.isTemporary()) {
					continue;
				}
				// log.debug("removing from:"+q.getTemporaryTableName());
				// log.debug("number:"+i);
				List<Column> outputs = new ArrayList<Column>();
				for (Output o : q.getOutputs()) {
					outputs.add(new Column(q.getTemporaryTableName(), o.getOutputName()));
				}
				// log.debug("outputs:"+outputs);
				for (int j = qs.size() - 1; j > i; j--) {
					if (qs.get(j).containsIputTable(q.getTemporaryTableName())) {
						if (qs.get(j).getOutputs().isEmpty()) {
							outputs.clear();
							break;
						}
						// else{
						/// queriesUsingQ.add(qs.get(j));
						// }
					}
					List<Column> cols = qs.get(j).getAllColumns();
					for (Column c2 : cols) {
						// log.debug(c2);
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
				Set<HashCode> removedOutputs = new HashSet<HashCode>();
				for (Column c3 : outputs) {
					for (int k = 0; k < q.getOutputs().size(); k++) {
						Output o = q.getOutputs().get(k);
						if (o.getOutputName().equals(c3.getName())) {
							Output out = q.getOutputs().get(k);
							log.debug("removing output:" + o);
							q.getOutputs().remove(k);
							removedOutputs.add(out.getHashID());
							// q.setHashId(null);
							k--;
						}
					}
				}
				if (!removedOutputs.isEmpty() && q.getHashId() != null) {
					HashCode outputsAll = Hashing.combineUnordered(removedOutputs);
					List<HashCode> newHashForQuery = new ArrayList<HashCode>();
					newHashForQuery.add(q.getHashId());
					newHashForQuery.add(Hashing.sha1().hashBytes("removing outputs".getBytes()));
					newHashForQuery.add(outputsAll);
					q.setHashId(Hashing.combineOrdered(newHashForQuery));
					/*
					 * for(SQLQuery using:queriesUsingQ){ List<HashCode>
					 * newHashForQueryUsing=new ArrayList<HashCode>();
					 * newHashForQueryUsing.add(using.getHashId());
					 * newHashForQueryUsing.add(Hashing.sha1().hashBytes(
					 * "removing outputs".getBytes()));
					 * newHashForQueryUsing.add(outputsAll);
					 * using.setHashId(Hashing.combineOrdered(
					 * newHashForQueryUsing)); }
					 */
					

				}
				if (removedOutputs.isEmpty() && q.existsInCache()) {
					// remove q and replace with the cached tablename
					String tablename = q.getInputTables().get(0).getName();
					String qName = q.getTemporaryTableName();
					for (int j = qs.size() - 1; j > i; j--) {
						for (Table t : qs.get(j).getInputTables()) {
							if (t.getName().equals(qName)) {
								t.setName(tablename);
							}
						}
					}
					qs.remove(i);

				}
			}
		}
		
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

	

		if (e.getObject() instanceof Table) {
			Table t = (Table) k.getNode().getObject();
			tempResult.setLastTable(t);
			

			tempResult.trackBaseTableFromQuery(t.getAlias(), t.getAlias());

			return;
		}

		Node op = e.getChildAt(p.getChoice());
		SQLQuery old = current;
		if (memo.getMemoValue(k).isMaterialised()) {

			SQLQuery q2 = new SQLQuery();
			q2.setHashId(e.computeHashIDExpand());

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
				//Table t = (Table) e.getObject();
				//current.setTemporaryTableName(t.getName());
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
			//List<String> inputNames = new ArrayList<String>();
			for (int j = 0; j < op.getChildren().size(); j++) {

				combineOperatorsAndOutputQueriesCentralized(p.getInputPlan(j), tempResult, visited);
				
				Table last=tempResult.getLastTable();
				String lastRes = last.getAlias();
				
				if(j==1&&nuwc.isRightinv()){
					last.setName("inv"+last.getName());
				}
				
				if (lastRes != current.getTemporaryTableName()
						&& !current.getInputTables().contains(last)) {
					//if(j==1&&nuwc.isRightinv()){
					//	Table inv=new Table("inv"+last.getName(), last.getAlias());
					//	current.addInputTableIfNotExists(inv);
					//}
				//	else{
						current.addInputTableIfNotExists(last);
					//}
					addOutputs(current, lastRes, tempResult);
					
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
					// for (Operand o : bwc.getOperands()) {
					for (Column c : bwc.getAllColumnRefs()) {
						// List<Column> cs = o.getAllColumnRefs();
						// if (!cs.isEmpty()) {
						// not constant
						// Column c = cs.get(0);

						if (op.getChildAt(j).isDescendantOfBaseTable(c.getAlias())) {
							bwc.changeColumn(c, new Column(tempResult.getQueryForBaseTable(c.getAlias()), c.getName(),
									c.getAlias()));
							// addOutputIfNotExists(c, tempResult);
						}
					}

				}
			}
	


		} else if (op.getOpCode() == Node.UNION || op.getOpCode() == Node.UNIONALL) {
			List<SQLQuery> unions = new ArrayList<SQLQuery>();
			if (e.getParents().isEmpty()) {
				current.setHashId(e.computeHashIDExpand());
			}
			for (int l = 0; l < op.getChildren().size(); l++) {
				SQLQuery u = new SQLQuery();

				// visited.put(op, current);
				tempResult.setCurrent(u);
				combineOperatorsAndOutputQueriesCentralized(p.getInputPlan(l), tempResult, visited);
				if (memo.getMemoValue(p.getInputPlan(l)).isMaterialised()) {
					u = tempResult.get(tempResult.getLastTable().getAlias());
					
				} else {
					// visited.put(p.getInputPlan(l), u);
					tempResult.add(u);
				}
				u.setHashId(p.getInputPlan(l).getNode().computeHashIDExpand());
				
				unions.add(u);

			}
			if (op.getChildren().size() > 1) {
				tempResult.add(current);
				current.setUnionqueries(unions);
				if (op.getOpCode() == Node.UNION) {
					current.setUnionAll(false);
				}
				visited.put(k, current);
				current.setHashId(e.computeHashIDExpand());
				if (memo.getMemoValue(k).isMaterialised()) {
					current.setMaterialised(true);
				}
			} else {
				visited.put(k, unions.get(0));
				unions.get(0).setHashId(p.getInputPlan(0).getNode().computeHashIDExpand());
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
						// if (op.isDescendantOfBaseTable(c.getAlias())) {
						uwcCloned.changeColumn(c,
								new Column(tempResult.getQueryForBaseTable(c.getAlias()), c.getName(), c.getAlias()));
						// addOutputIfNotExists(c, tempResult);
						// }
					}
					current.addUnaryWhereCondition(uwcCloned);
				} else {
					NonUnaryWhereCondition nuwc = (NonUnaryWhereCondition) o;
					NonUnaryWhereCondition nuwcCloned = nuwc.clone();
					nuwcCloned.addRangeFilters(nuwc);
					if (!inputName.equals(current.getTemporaryTableName())) {
						for (Column c : nuwcCloned.getAllColumnRefs()) {
							// if (op.isDescendantOfBaseTable(c.getAlias())) {
							nuwcCloned.changeColumn(c, new Column(tempResult.getQueryForBaseTable(c.getAlias()),
									c.getName(), c.getAlias()));
							// addOutputIfNotExists(c, tempResult);
							// }
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
			current.setHashId(p.getInputPlan(0).getNode().computeHashIDExpand());
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
			for (Column c : orderCols) {
				c.setAlias(null);
			}
			if (!tempResult.getLastTable().getAlias().equals(current.getTemporaryTableName())) {
				current.addInputTableIfNotExists(tempResult.getLastTable());
			}

		} else if (op.getOpCode() == Node.GROUPBY) {
			combineOperatorsAndOutputQueriesCentralized(p.getInputPlan(0), tempResult, visited);

			List<Column> groupCols = (ArrayList<Column>) op.getObject();
			current.setGroupBy(groupCols);
			// for(Column c:groupCols){
			// c.setAlias(null);
			// }
			if (!tempResult.getLastTable().getAlias().equals(current.getTemporaryTableName())) {
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
			current.setHashId(e.computeHashIDExpand());
			log.debug("cardinality estimation for " + current.getTemporaryTableName() + ":");
			if (e.getNodeInfo() != null) {
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
		} 
	}



	
}


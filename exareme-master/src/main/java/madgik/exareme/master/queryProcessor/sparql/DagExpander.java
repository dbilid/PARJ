package madgik.exareme.master.queryProcessor.sparql;

import madgik.exareme.master.queryProcessor.decomposer.dag.Node;
import madgik.exareme.master.queryProcessor.decomposer.dag.NodeHashValues;
import madgik.exareme.master.queryProcessor.decomposer.federation.CentralizedMemoValue;
import madgik.exareme.master.queryProcessor.decomposer.federation.Memo;
import madgik.exareme.master.queryProcessor.decomposer.federation.MemoKey;
import madgik.exareme.master.queryProcessor.decomposer.federation.SinglePlan;
import madgik.exareme.master.queryProcessor.decomposer.query.NonUnaryWhereCondition;
import madgik.exareme.master.queryProcessor.decomposer.query.Table;
import madgik.exareme.master.queryProcessor.decomposer.util.Util;
import madgik.exareme.master.queryProcessor.estimator.NodeCostEstimator;

public class DagExpander {
	private Node root;
	private NodeHashValues hashes;
	private long startTime;
	private long expandDagTime=200000L;
	private NodeCostEstimator nce;
	
	public DagExpander(Node root, NodeHashValues hashes) {
		super();
		this.root = root;
		this.hashes = hashes;
		nce=new NodeCostEstimator(1);
	}
	
	public void expand(){
		startTime=System.currentTimeMillis();
		expandDAG(root);
	}
	
	private void expandDAG(Node eq) {

		if (System.currentTimeMillis() - startTime > expandDagTime) {
			return;
		}

		for (int i = 0; i < eq.getChildren().size(); i++) {
			// System.out.println(eq.getChildren().size());
			Node op = eq.getChildAt(i);
			
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
				if (op.getObject() instanceof NonUnaryWhereCondition && op.isCommutativity()) {
					boolean useCommutativity = true;
					NonUnaryWhereCondition bwc = (NonUnaryWhereCondition) op.getObject();
					if (bwc.getOperator().equals("=")) {
						
						if (useCommutativity) {
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

							if (!hashes.containsKey(commutativity.computeHashIDExpand())) {
								hashes.put(commutativity.computeHashIDExpand(), commutativity);
								// hashes.remove(eq.computeHashIDExpand());
								// for (Node p : eq.getParents()) {
								// hashes.remove(p.computeHashIDExpand());
								// }

								eq.addChild(commutativity);
								commutativity.setCommutativity(false);
								// hashes.put(eq.computeHashIDExpand(), eq);
								commutativity.addAllDescendantBaseTables(op.getDescendantBaseTables());
								

								// for (Node p : eq.getParents()) {
								// hashes.put(p.computeHashID(), p);
								// }
							} else {
								unify(eq, hashes.get(commutativity.computeHashIDExpand()).getFirstParent());
								commutativity.removeAllChildren();
								hashes.get(commutativity.computeHashIDExpand()).setCommutativity(false);

							}
						}
					}
				}

				// join swap
				// (A join B) join C -> (A join C) join B
				if ( op.getObject() instanceof NonUnaryWhereCondition && op.isSwap()) {
					NonUnaryWhereCondition bwc = (NonUnaryWhereCondition) op.getObject();
					if (bwc.getOperator().equals("=")) {
						// for (Node c2 : op.getChildren()) {
						if (op.getChildren().size() > 1) {
							Node c2 = op.getChildAt(0);
							for (Node c3 : c2.getChildren()) {
								boolean childrenBase = c3.getDescendantBaseTables().size() == 2
										&& c3.getChildren().size() == 2;
								// if (c2.getChildren().size() > 0) {
								// Node c3 = c2.getChildAt(0);
								if (c3.getObject() instanceof NonUnaryWhereCondition) {
									NonUnaryWhereCondition bwc2 = (NonUnaryWhereCondition) c3.getObject();
									if (bwc2.getOperator().equals("=")) {
										boolean comesFromLeftOp = c3.getChildAt(0).isDescendantOfBaseTable(
												bwc.getLeftOp().getAllColumnRefs().get(0).getAlias());
										if (!comesFromLeftOp)
											continue;
										Node associativity = new Node(Node.AND, Node.JOIN);
										NonUnaryWhereCondition newBwc = new NonUnaryWhereCondition();
										newBwc.setOperator("=");

										newBwc.setRightOp(bwc.getRightOp());
										newBwc.setLeftOp(bwc.getLeftOp());
										newBwc.addRangeFilters(bwc);
										associativity.setObject(newBwc);

										if (comesFromLeftOp) {
											associativity.addChild(c3.getChildAt(0));

										} else {
											associativity.addChild(c3.getChildAt(1));

										}

										associativity.addChild(op.getChildAt(1));

									

										Node table = new Node(Node.OR);
										if (hashes.containsKey(associativity.computeHashIDExpand()) && !hashes
												.get(associativity.computeHashIDExpand()).getParents().isEmpty()) {
											// if
											// (hashes.containsKey(associativity.computeHashIDExpand()))
											// {
											Node assocInHashes = hashes.get(associativity.computeHashIDExpand());
											table = assocInHashes.getFirstParent();

											
											associativity.removeAllChildren();
											// associativity = assocInHashes;

										} else {
											hashes.put(associativity.computeHashIDExpand(), associativity);
											table.addChild(associativity);

											hashes.put(table.computeHashIDExpand(), table);
											associativity.addAllDescendantBaseTables(
													op.getChildAt(1).getDescendantBaseTables());
											

											if (comesFromLeftOp) {
												associativity.addAllDescendantBaseTables(
														c3.getChildAt(0).getDescendantBaseTables());

											} else {
												associativity.addAllDescendantBaseTables(
														c3.getChildAt(1).getDescendantBaseTables());

											}
											table.addAllDescendantBaseTables(associativity.getDescendantBaseTables());
										}

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
										if (!hashes.containsKey(associativityTop.computeHashIDExpand()) || hashes
												.get(associativityTop.computeHashIDExpand()).getParents().isEmpty()) {
											hashes.put(associativityTop.computeHashIDExpand(), associativityTop);
											// Node newTop =
											// hashes.checkAndPutWithChildren(associativityTop);
											// hashes.remove(eq.computeHashIDExpand());
											// for (Node p : eq.getParents()) {
											// hashes.remove(p.computeHashIDExpand());
											// }
											associativityTop.setSwap(false);
											eq.addChild(associativityTop);
											associativityTop.addAllDescendantBaseTables(op.getDescendantBaseTables());
											

											

										} else {

											unify(eq, hashes.get(associativityTop.computeHashIDExpand())
													.getFirstParent());
											// same as unify(eq', eq)???
											// checking again children of eq?
											associativityTop.removeAllChildren();
											hashes.get(associativityTop.computeHashIDExpand()).setSwap(false);
											if (table.getParents().isEmpty()) {
												if (hashes.get(table.computeHashIDExpand()) == table) {
													hashes.remove(table.computeHashIDExpand());
												}
												for (Node n : table.getChildren()) {
													if (n.getParents().size() == 1) {
														if (hashes.get(n.computeHashIDExpand()) == n) {
															hashes.remove(n.computeHashIDExpand());
														}
													}
												}
												table.removeAllChildren();
											}
											if (associativity.getParents().isEmpty()) {
												if (hashes.get(associativity.computeHashIDExpand()) == associativity) {
													hashes.remove(associativity.computeHashIDExpand());
												}
												associativity.removeAllChildren();
											}
										}
									}
								}
							}
						}

						else {
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

										if (hashes.containsKey(associativity.computeHashIDExpand()) && !hashes
												.get(associativity.computeHashIDExpand()).getParents().isEmpty()) {
											Node assocInHashes = hashes.get(associativity.computeHashIDExpand());
											table = assocInHashes.getFirstParent();
											
											associativity.removeAllChildren();
											// associativity = assocInHashes;

										} else {
											hashes.put(associativity.computeHashIDExpand(), associativity);
											table.addChild(associativity);

											// table.setPartitionedOn(new
											// PartitionCols(newBwc.getAllColumnRefs()));
											hashes.put(table.computeHashIDExpand(), table);

											

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
										if (!hashes.containsKey(associativityTop.computeHashIDExpand()) || hashes
												.get(associativityTop.computeHashIDExpand()).getParents().isEmpty()) {
											hashes.put(associativityTop.computeHashIDExpand(), associativityTop);
											// Node newTop =
											// hashes.checkAndPutWithChildren(associativityTop);
											// hashes.remove(eq.computeHashIDExpand());
											// for (Node p : eq.getParents()) {
											// hashes.remove(p.computeHashIDExpand());
											// }
											eq.addChild(associativityTop);
											associativityTop.addAllDescendantBaseTables(op.getDescendantBaseTables());
											
											
										} else {

											unify(eq, hashes.get(associativityTop.computeHashIDExpand())
													.getFirstParent());
											// same as unify(eq', eq)???
											// checking again children of eq?
											associativityTop.removeAllChildren();
											if (table.getParents().isEmpty()) {
												if (hashes.get(table.computeHashIDExpand()) == table) {
													hashes.remove(table.computeHashIDExpand());
												}
												for (Node n : table.getChildren()) {
													if (n.getParents().size() == 1) {
														if (hashes.get(n.computeHashIDExpand()) == n) {
															hashes.remove(n.computeHashIDExpand());
														}
													}
												}
												table.removeAllChildren();
											}
											if (associativity.getParents().isEmpty()) {
												if (hashes.get(associativity.computeHashIDExpand()) == associativity) {
													hashes.remove(associativity.computeHashIDExpand());
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
					op.computeHashIDExpand();
				}
				op.setExpanded(true);

			}
		}
		// eq.computeHashID();

	}

	private void unify(Node q, Node q2) {
		if (q == q2) {
			return;
		}

		// hashes.remove(q2.computeHashIDExpand());
		// hashes.remove(q.computeHashIDExpand());
		// for (Node p : q.getParents()) {
		/// hashes.remove(p.computeHashIDExpand());
		// p.setHashNeedsRecomputing();
		// }
		q.getUnions().addAll(q2.getUnions());

		for (Node c : q2.getChildren()) {
			q.addChild(c);
			q.addAllDescendantBaseTables(c.getDescendantBaseTables());
		}
		// for (Node p : q.getParents()) {
		// hashes.put(p.computeHashIDExpand(), p);
		// }
		q2.removeAllChildren();
		for (int i = 0; i < q2.getParents().size(); i++) {
			Node p = q2.getParents().get(i);
			// System.out.println(p.computeHashIDExpand());
			hashes.remove(p.computeHashIDExpand());
			int pos = p.removeChild(q2);
			i--;
			if (p.getParents().isEmpty()) {
				continue;
			}
			p.addChildAt(q, pos);
			if (hashes.containsKey(p.computeHashIDExpand(true))
					&& !hashes.get(p.computeHashIDExpand()).getParents().isEmpty()) {
				// System.out.println("further unification!");
				unify(hashes.get(p.computeHashIDExpand()).getFirstParent(), p.getFirstParent());
			} else {
				hashes.put(p.computeHashIDExpand(), p);
			}
			// System.out.println(p.computeHashIDExpand());
		}
		// hashes.put(q.computeHashIDExpand(), q);
	}
	
	
	public SinglePlan getBestPlanCentralized(Node e, double limit, Memo memo) {
		MemoKey ec = new MemoKey(e, null);
		SinglePlan resultPlan;
		if (memo.containsMemoKey(ec) && memo.getMemoValue(ec).isMaterialised()) {
			// check on c!
			resultPlan = new SinglePlan(0, null);
		} else if (memo.containsMemoKey(ec)) {
			CentralizedMemoValue cmv = (CentralizedMemoValue) memo.getMemoValue(ec);
			 
				resultPlan = memo.getMemoValue(ec).getPlan();
				
			
		} else {
			resultPlan = searchForBestPlanCentralized(e, limit, memo);
			CentralizedMemoValue cmv = (CentralizedMemoValue) memo.getMemoValue(ec);
		
		}
		if (resultPlan != null && resultPlan.getCost() < limit) {
			return resultPlan;
		} else {
			return null;
		}
	}

	private SinglePlan searchForBestPlanCentralized(Node e, double limit, Memo memo) {

		
		if (e.getObject() instanceof Table) {
			// base table
			SinglePlan r = new SinglePlan(0);

			memo.put(e, r, true, true, false);

			return r;
		}

		SinglePlan resultPlan = new SinglePlan(Double.MAX_VALUE);

		for (int k = 0; k < e.getChildren().size(); k++) {
			Node o = e.getChildAt(k);
			SinglePlan e2Plan = new SinglePlan(Double.MAX_VALUE);
			Double opCost = nce.getCostForOperator(o);
			boolean fed = false;
			boolean mat = false;
			// this must go after algorithmic implementation
			limit -= opCost;
			// if (limit < 0) {
			// continue;
			// }
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


				SinglePlan t = getBestPlanCentralized(e2, algLimit, memo);
				algPlan.addInputPlan(e2, null);
				algPlan.increaseCost(t.getCost());

				CentralizedMemoValue cmv = (CentralizedMemoValue) memo.getMemoValue(new MemoKey(e2, null));
				if (o.getOpCode() == Node.NESTED) {
					mat = true;
					Util.setDescNotMaterialised(e2, memo);
				}
				if (o.getOpCode() == Node.LEFTJOIN || o.getOpCode() == Node.JOINKEY) {
					cmv.setMaterialized(true);
				}
				if (cmv.isFederated()) {
					if (o.getOpCode() == Node.JOIN) {
						cmv.setMaterialized(true);
					} else {
						fed = true;
					}
				}

				algLimit -= algPlan.getCost();
				// if(algLimit<0){
				// continue;
				// }

			}

			if (algPlan.getCost() < e2Plan.getCost()) {
				e2Plan = algPlan;
			}

			// }
			if (e2Plan.getCost() < resultPlan.getCost()) {
				resultPlan = e2Plan;
				memo.put(e, resultPlan, mat, false, fed);
				/*
				 * if (!e.getParents().isEmpty() &&
				 * (e.getParents().get(0).getOpCode() == Node.UNION ||
				 * e.getParents().get(0).getOpCode() == Node.UNIONALL)) {
				 * 
				 * limit = resultPlan.getCost(); System.out.println("prune: " +
				 * e.getObject() + "with limit:" + limit); }
				 */

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
			
			memo.setPlanUsed(new MemoKey(e, null));
			// e.setMaterialised(true);
			// e.setPlanMaterialized(resultPlan.getPath().getPlanIterator());
		}
		return resultPlan;

	}
	
}

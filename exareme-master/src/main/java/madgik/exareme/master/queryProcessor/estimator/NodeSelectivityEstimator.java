package madgik.exareme.master.queryProcessor.estimator;

import com.google.gson.Gson;

import madgik.exareme.master.queryProcessor.analyzer.stat.JoinCardinalities;
import madgik.exareme.master.queryProcessor.analyzer.stat.StatUtils;
import madgik.exareme.master.queryProcessor.decomposer.dag.Node;
import madgik.exareme.master.queryProcessor.decomposer.query.*;
import madgik.exareme.master.queryProcessor.estimator.db.RelInfo;
import madgik.exareme.master.queryProcessor.estimator.db.Schema;
import madgik.exareme.master.queryProcessor.estimator.histogram.Histogram;
import madgik.exareme.master.queryProcessor.estimator.db.AttrInfo;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author jim
 */
public class NodeSelectivityEstimator {
	private static final int HASH_STRING_CHARS = 11;
	private static final int HASH_STRING_BASE = 256;

	private Schema schema;
	private int rdfTypeTable;

	private static final org.apache.log4j.Logger log = org.apache.log4j.Logger
			.getLogger(NodeSelectivityEstimator.class);

	public NodeSelectivityEstimator(String json) throws Exception {
		BufferedReader br;
		br = new BufferedReader(new FileReader(json));

		// convert the json string back to object
		Gson gson = new Gson();
		schema = gson.fromJson(br, Schema.class);
		

		// System.out.println(schema);

		// HashMap<String, HashSet<String>> keys = new HashMap<String,
		// HashSet<String>>();
		// try {
		// keys =
		// di.madgik.decomposer.util.Util.getMysqlIndices("jdbc:mysql://10.240.0.10:3306/npd?"
		// + "user=benchmark&password=pass");
		// } catch (SQLException ex) {
		// Logger.getLogger(NodeSelectivityEstimator.class.getName()).log(Level.SEVERE,
		// null, ex);
		// }
		// for (String table : keys.keySet()) {
		// if(schema.getTableIndex().containsKey(table)){
		// schema.getTableIndex().get(table).setHashAttr(keys.get(table));
		// schema.getTableIndex().get(table).setNumberOfPartitions(1);}
		// else{
		// Logger.getLogger(NodeSelectivityEstimator.class.getName()).log(Level.WARNING,
		// "Table {0} does not exist in stat json file", table);
		// }
		// }

	}

	public void makeEstimationForNode(Node n) {
		if (n.getObject() instanceof Table) {
			estimateBase(n);
		} else {
			Node o = n.getChildAt(0);
			if (o.getOpCode() == Node.JOIN) {
				NonUnaryWhereCondition bwc = (NonUnaryWhereCondition) o.getObject();
				if (o.getChildren().size() == 1) {
					estimateFilterJoin(n, bwc, o.getChildAt(0), o.getChildAt(0));
				} else {
					estimateJoin(n, bwc, o.getChildAt(0), o.getChildAt(1));
				}
			} else if (o.getOpCode() == Node.PROJECT || o.getOpCode() == Node.BASEPROJECT) {
				estimateProject(n);
			} else if (o.getOpCode() == Node.SELECT) {
				Selection s = (Selection) o.getObject();
				estimateFilter(n, s, o.getChildAt(0));
			} else if (o.getOpCode() == Node.UNION || o.getOpCode() == Node.UNIONALL) {
				estimateUnion(n);
			} else if (o.getOpCode() == Node.NESTED) {
				NodeInfo nested = new NodeInfo();
				String nestedAlias = n.getDescendantBaseTables().iterator().next();
				RelInfo rel = o.getChildAt(0).getNodeInfo().getResultRel();
				// RelInfo rel =
				// this.planInfo.get(n.getHashId()).getResultRel();
				RelInfo resultRel = new RelInfo(rel, -1, true);
				nested.setNumberOfTuples(rel.getNumberOfTuples());
				nested.setTupleLength(rel.getTupleLength());
				nested.setResultRel(resultRel);
				n.setNodeInfo(nested);

			} else if (o.getOpCode() == Node.GROUPBY) {
				estimateGroupBy(n);
			} else if (o.getOpCode() == Node.LEFTJOIN) {
				// TODO compute selectivity for left join
				// for now we get only one condition and treat it as normal join
				BinaryOperand bo = (BinaryOperand) o.getObject();
				NonUnaryWhereCondition nuwc = QueryUtils.getJoinCondition(bo, o);
				estimateJoin(n, nuwc, o.getChildAt(0), o.getChildAt(1));

			} else if (o.getOpCode() == Node.ORDERBY) {
				estimateOrderBy(n);
			} else if (o.getOpCode() == Node.LIMIT) {
				estimateLimit(n);
			}
		}

	}

	private void estimateLimit(Node n) {
		NodeInfo nested = new NodeInfo();
		RelInfo rel = n.getChildAt(0).getChildAt(0).getNodeInfo().getResultRel();
		// RelInfo rel =
		// this.planInfo.get(n.getHashId()).getResultRel();
		Integer limit = (Integer) n.getChildAt(0).getObject();
		RelInfo resultRel = new RelInfo(rel);
		if (limit.doubleValue() > rel.getNumberOfTuples()) {
			nested.setNumberOfTuples(rel.getNumberOfTuples());
		} else {
			nested.setNumberOfTuples(limit.doubleValue());
		}

		nested.setTupleLength(rel.getTupleLength());
		nested.setResultRel(resultRel);
		n.setNodeInfo(nested);

	}

	private void estimateOrderBy(Node n) {
		NodeInfo nested = new NodeInfo();
		RelInfo rel = n.getChildAt(0).getChildAt(0).getNodeInfo().getResultRel();
		// RelInfo rel =
		// this.planInfo.get(n.getHashId()).getResultRel();
		RelInfo resultRel = new RelInfo(rel);
		nested.setNumberOfTuples(rel.getNumberOfTuples());
		nested.setTupleLength(rel.getTupleLength());
		nested.setResultRel(resultRel);
		n.setNodeInfo(nested);

	}

	private void estimateGroupBy(Node n) {
		// TODO estimate group by
		NodeInfo nested = new NodeInfo();
		RelInfo rel = n.getChildAt(0).getChildAt(0).getNodeInfo().getResultRel();
		// RelInfo rel =
		// this.planInfo.get(n.getHashId()).getResultRel();
		RelInfo resultRel = new RelInfo(rel);
		nested.setNumberOfTuples(rel.getNumberOfTuples());
		nested.setTupleLength(rel.getTupleLength());
		nested.setResultRel(resultRel);
		n.setNodeInfo(nested);

	}

	public void estimateFilter(Node n, Selection s, Node child) {
		// Selection s = (Selection) n.getObject();
		if (isRDFType(child) && s.getOperands().size() == 1 && !s.getAllColumnRefs().get(0).getName()) {
			Constant c = null;
			Operand op = s.getOperands().iterator().next();
			if (op instanceof NonUnaryWhereCondition) {
				NonUnaryWhereCondition nuwc = (NonUnaryWhereCondition) op;
				if (nuwc.getRightOp() instanceof Constant) {
					c = (Constant) nuwc.getRightOp();
				} else if (nuwc.getLeftOp() instanceof Constant) {
					c = (Constant) nuwc.getLeftOp();
				} else {
					log.error("no constant operand found:" + nuwc);
					return;
				}
				NodeInfo pi = new NodeInfo();
				Table tbl = (Table) child.getObject();
				long value = (long) c.getValue();
				int intValue = (int) value;
				RelInfo rel = this.schema.getTableIndex().get(-intValue);
				// RelInfo rel =
				// this.planInfo.get(n.getHashId()).getResultRel();

				// System.out.println(rel);
				RelInfo resultRel = new RelInfo(rel, tbl.getAlias(), false);
				/*
				 * Map<String, AttrInfo> aliasAtts=new HashMap<String,
				 * AttrInfo>(); for(String
				 * colname:resultRel.getAttrIndex().keySet()){
				 * aliasAtts.put(tableAlias+"."+colname,
				 * resultRel.getAttrIndex().get(colname)); }
				 * resultRel.setAttrIndex(aliasAtts);
				 */

				// TODO: fix nodeInfo
				pi.setNumberOfTuples(rel.getNumberOfTuples());
				pi.setTupleLength(rel.getTupleLength());
				pi.setResultRel(resultRel);
				n.setNodeInfo(pi);
				return;
			} else {
				log.error("filter on rdf type table not NUWC:" + op);
				return;
			}

		}
		NodeInfo ni = new NodeInfo();
		n.setNodeInfo(ni);
		NodeInfo childInfo = child.getNodeInfo();

		Set<Operand> filters = s.getOperands();

		// RelInfo initRel = childInfo.getResultRel();
		ni.setNumberOfTuples(childInfo.getNumberOfTuples());
		ni.setTupleLength(childInfo.getTupleLength());
		ni.setResultRel(new RelInfo(childInfo.getResultRel()));

		// one select node can contain more than one filter!
		for (Operand nextFilter : filters) {
			applyFilterToNode(nextFilter, ni, child);
		}

	}

	private boolean isRDFType(Node child) {
		if (child.getObject() instanceof Table) {
			Table tbl = (Table) child.getObject();
			if (tbl.getName() == rdfTypeTable) {
				return true;
			}
		}
		return false;
	}

	private void applyFilterToNode(Operand nextFilter, NodeInfo ni, Node child) {
		if (nextFilter instanceof UnaryWhereCondition) {
			UnaryWhereCondition uwc = (UnaryWhereCondition) nextFilter;
			if (uwc.getType() == UnaryWhereCondition.LIKE) {
				// for now treat like equality
				// TODO treat properly
				try {
					Column col = uwc.getAllColumnRefs().get(0);
					String con = uwc.getValue();
					if (!ni.getResultRel().getAttrIndex().containsKey(col)) {
						AttrInfo att = getAttributeFromBase(col, child, ni);
						if (att == null) {
							log.error("Column not found in Attribute index: " + col.toString());
							ni.setNumberOfTuples(ni.getResultRel().getNumberOfTuples());
							return;
						} else {
							ni.getResultRel().getAttrIndex().put(col, att);
						}
					}
					Histogram resultHistogram = ni.getResultRel().getAttrIndex().get(col).getHistogram();

					double filterValue = 0;

					filterValue = StatUtils.hashString(con);
					String newSt = "";

					// if (con.startsWith("\'")) {
					newSt = con.replaceAll("\'", "").replaceAll("%", "");
					if (uwc.getOperand().toString().toLowerCase().contains("lower")) {
						newSt = newSt.toUpperCase();
					}
					if (uwc.getOperand().toString().toLowerCase().contains("upper")) {
						newSt = newSt.toLowerCase();
					}
					filterValue = StatUtils.hashString(newSt);
					// }
					log.debug("LIKE operator, removing % :" + newSt);
					resultHistogram.equal(filterValue);

					ni.getResultRel().adjustRelation(col, resultHistogram);

					// TODO: fix NOdeInfo!!
					ni.setNumberOfTuples(ni.getResultRel().getNumberOfTuples());
				} catch (Exception e) {
					log.error("Could not compute selectivity for filter: " + nextFilter);
				}

			}
			// normally you don't care for these conditions (Column IS NOT
			// NULL)
			// UnaryWhereCondition uwc = (UnaryWhereCondition) nextFilter;
			// Table t=(Table) child.getObject();
			// if(t.getName().startsWith("table")){
			// not base table

			// TODO: fix nodeInfo
			// do nothing!

			// this.planInfo.get(n.getHashId()).setNumberOfTuples(child.getNumberOfTuples());
			// this.planInfo.get(n.getHashId()).setTupleLength(child.getTupleLength());
			// System.out.println(uwc);
		} else if (nextFilter instanceof NonUnaryWhereCondition) {
			// TODO correct this!
			NonUnaryWhereCondition nuwc = (NonUnaryWhereCondition) nextFilter;
			// System.out.println(nuwc);
			String operator = nuwc.getOperator(); // e.g. =, <, >

			if (operator.equalsIgnoreCase("and")) {
				applyFilterToNode(nuwc.getLeftOp(), ni, child);
				applyFilterToNode(nuwc.getRightOp(), ni, child);
			} else if (operator.equalsIgnoreCase("or")) {
				log.warn("Filter with OR condition " + nuwc.toString()
						+ ". Selectivity estimation will not be accurate");
				// TODO fix estimation with OR conditions
				// for now just return the one with higher cardinality
				NodeInfo left = new NodeInfo();
				left.setNumberOfTuples(ni.getNumberOfTuples());
				left.setTupleLength(ni.getTupleLength());
				left.setResultRel(new RelInfo(ni.getResultRel()));
				applyFilterToNode(nuwc.getLeftOp(), left, child);

				NodeInfo right = new NodeInfo();
				right.setNumberOfTuples(ni.getNumberOfTuples());
				right.setTupleLength(ni.getTupleLength());
				right.setResultRel(new RelInfo(ni.getResultRel()));
				applyFilterToNode(nuwc.getLeftOp(), right, child);

				ni = left.getNumberOfTuples() > right.getNumberOfTuples() ? left : right;
			} else {

				Column col;
				Constant con;

				if (nuwc.getLeftOp() instanceof Column) {// TODO: constant
					col = (Column) nuwc.getLeftOp();
					con = (Constant) nuwc.getRightOp();
				} else {
					col = (Column) nuwc.getRightOp();
					con = (Constant) nuwc.getLeftOp();
				}

				// RelInfo lRel =
				// this.schema.getTableIndex().get(col.tableAlias);
				// RelInfo lRel = childInfo.getResultRel();
				// RelInfo resultRel = new RelInfo(lRel);
				// RelInfo resultRel = initRel;

				// check to see if it is "cut" by base projection
				if (!ni.getResultRel().getAttrIndex().containsKey(col)) {
					AttrInfo att = getAttributeFromBase(col, child, ni);
					if (att == null) {
						log.error("Column not found in Attribute index: " + col.toString());
						ni.setNumberOfTuples(ni.getResultRel().getNumberOfTuples());
						return;
					} else {
						ni.getResultRel().getAttrIndex().put(col, att);
					}

				}
				Histogram resultHistogram = ni.getResultRel().getAttrIndex().get(col).getHistogram();

				double filterValue = 0;
				/*
				 * if (!con.isArithmetic()) { if (con.getValue() instanceof
				 * String) { String st = (String) con.getValue(); filterValue =
				 * StatUtils.hashString(con.getValue().toString()); String newSt
				 * = ""; if (st.startsWith("\'")) { newSt = st.replaceAll("\'",
				 * ""); filterValue = StatUtils.hashString(newSt); }
				 * 
				 * }
				 * 
				 * } else {
				 */
				filterValue = Double.parseDouble(con.getValue().toString());
				// }

				if (operator.equals("="))
					resultHistogram.equal(filterValue);
				else if (operator.equals(">="))
					resultHistogram.greaterOrEqual(filterValue);
				else if (operator.equals("<="))
					resultHistogram.lessOrEqualValueEstimation(filterValue);
				else if (operator.equals(">"))
					resultHistogram.greaterThan(filterValue);
				else if (operator.equals("<"))
					resultHistogram.lessThanValueEstimation(filterValue);
				// else f = new Filter(col.tableAlias, col.columnName,
				// FilterOperand.NotEqual, Double.parseDouble(con.toString()));

				// adjust RelInfo's histograms based on the resulting histogram
				ni.getResultRel().adjustRelation(col, resultHistogram);

				// TODO: fix NOdeInfo!!
				ni.setNumberOfTuples(ni.getResultRel().getNumberOfTuples());
				// ni.setTupleLength(ni.getResultRel().getTupleLength());
				// ni.setResultRel(resultRel);
			}
		}

	}

	private AttrInfo getAttributeFromBase(Column col, Node child, NodeInfo ni) {
		if (child.getChildren().size() == 0 || child.getChildAt(0).getOpCode() != Node.BASEPROJECT) {
			return null;
		}
		AttrInfo column = child.getChildAt(0).getChildAt(0).getNodeInfo().getResultRel().getAttrIndex().get(col);
		return column;
	}

	public void estimateJoin(Node n, NonUnaryWhereCondition nuwc, Node left, Node right) {
		// NonUnaryWhereCondition nuwc = (NonUnaryWhereCondition) n.getObject();
		NodeInfo ni = new NodeInfo();
		Column l = nuwc.getLeftOp().getAllColumnRefs().get(0);
		Column r = nuwc.getRightOp().getAllColumnRefs().get(0);
		if (!(nuwc.getLeftOp() instanceof Column)) {
			log.debug("Operand not Column. Selectivity estimation may not be accurate:" + nuwc.getLeftOp());
		}
		if (!(nuwc.getRightOp() instanceof Column)) {
			log.debug("Operand not Column. Selectivity estimation may not be accurate:" + nuwc.getRightOp());
		}

		// RelInfo lRel = this.schema.getTableIndex().get(l.tableAlias);
		// RelInfo rRel = this.schema.getTableIndex().get(r.tableAlias);
		RelInfo lRel = left.getNodeInfo().getResultRel();
		RelInfo rRel = right.getNodeInfo().getResultRel();

		RelInfo resultRel = new RelInfo(lRel);
		RelInfo newR = new RelInfo(rRel);

		Histogram resultHistogram = resultRel.getAttrIndex().get(l).getHistogram();
		if (newR.getNumberOfTuples() < 0.5 || lRel.getNumberOfTuples() < 0.5) {
			resultHistogram.convertToTransparentHistogram();
		} else {
			if (nuwc.getOperator().contains(">") || nuwc.getOperator().contains("<")) {
				resultHistogram.rangejoin(newR.getAttrIndex().get(r).getHistogram());
			} else {
				resultHistogram.join(newR.getAttrIndex().get(r).getHistogram(), -1);
			}

		}

		// lRel.getAttrIndex().get(l.columnName).getHistogram().join(rRel.getAttrIndex().get(r.columnName).getHistogram());

		// put all the right's RelInfo AttrInfos to the left one
		resultRel.getAttrIndex().putAll(newR.getAttrIndex());

		// adjust RelInfo's histograms based on the resulting histogram
		resultRel.adjustRelation(l, resultHistogram);

		// fix alias mappings to RelInfo. The joining aliases must point to the
		// same RelInfo after the join operation
		// schema.getTableIndex().put(l.toString(), resultRel);
		// schema.getTableIndex().put(r.toString(), resultRel);

		// adding necessary equivalent hashing attribures

		// TODO: fix nodeInfo
		ni.setNumberOfTuples(resultRel.getNumberOfTuples());
		ni.setTupleLength(resultRel.getTupleLength());
		ni.setResultRel(resultRel);
		n.setNodeInfo(ni);

		

	}

	public void estimateFilterJoin(Node n, NonUnaryWhereCondition nuwc, Node left, Node right) {
		// NonUnaryWhereCondition nuwc = (NonUnaryWhereCondition) n.getObject();
		NodeInfo ni = new NodeInfo();
		Column l = nuwc.getLeftOp().getAllColumnRefs().get(0);
		Column r = nuwc.getRightOp().getAllColumnRefs().get(0);
		if (!(nuwc.getLeftOp() instanceof Column)) {
			log.debug("Operand not Column. Selectivity estimation may not be accurate:" + nuwc.getLeftOp());
		}
		if (!(nuwc.getRightOp() instanceof Column)) {
			log.debug("Operand not Column. Selectivity estimation may not be accurate:" + nuwc.getRightOp());
		}
		// String equals = nuwc.getOperator();

		// RelInfo lRel = this.schema.getTableIndex().get(l.tableAlias);
		// RelInfo rRel = this.schema.getTableIndex().get(r.tableAlias);
		RelInfo lRel = left.getNodeInfo().getResultRel();
		RelInfo rRel = right.getNodeInfo().getResultRel();

		RelInfo resultRel = new RelInfo(lRel);

		Histogram resultHistogram = resultRel.getAttrIndex().get(l.toString()).getHistogram();

		if (rRel.getNumberOfTuples() < 0.5 || lRel.getNumberOfTuples() < 0.5) {
			resultHistogram.convertToTransparentHistogram();
		} else {
			resultHistogram.filterjoin(rRel.getAttrIndex().get(r.toString()).getHistogram());
		}

		// resultHistogram.filterjoin(rRel.getAttrIndex().get(r.getName()).getHistogram());

		// lRel.getAttrIndex().get(l.columnName).getHistogram().join(rRel.getAttrIndex().get(r.columnName).getHistogram());

		// put all the right's RelInfo AttrInfos to the left one
		resultRel.getAttrIndex().putAll(rRel.getAttrIndex());

		// adjust RelInfo's histograms based on the resulting histogram
		resultRel.adjustRelation(l, resultHistogram);

		// fix alias mappings to RelInfo. The joining aliases must point to the
		// same RelInfo after the join operation
		// schema.getTableIndex().put(l.toString(), resultRel);
		// schema.getTableIndex().put(r.toString(), resultRel);

		// adding necessary equivalent hashing attribures

		// TODO: fix nodeInfo
		ni.setNumberOfTuples(resultRel.getNumberOfTuples());
		ni.setTupleLength(resultRel.getTupleLength());
		ni.setResultRel(resultRel);
		n.setNodeInfo(ni);
	}

	public void estimateProject(Node n) {
		// String tableAlias;
		// NodeInfo ni = new NodeInfo();
		// n.setNodeInfo(ni);
		// Set<Column> columns = new HashSet<Column>();
		// Node prjNode = n.getChildAt(0);
		// Node child = prjNode.getChildAt(0);
		// Projection p = (Projection) prjNode.getObject();
		// List<Output> outputs = p.getOperands();
		// // tableAlias = ((Column)outputs.get(0).getObject()).tableAlias;
		//
		// // RelInfo rel = this.schema.getTableIndex().get(tableAlias);
		// if (child.getNodeInfo() == null) {
		// this.makeEstimationForNode(child);
		// }
		// RelInfo rel = child.getNodeInfo().getResultRel();
		//
		// RelInfo resultRel = new RelInfo(rel);
		//
		// for (Output o : outputs) {
		// List<Column> cols = o.getObject().getAllColumnRefs();
		// if (!cols.isEmpty()) {
		// Column c = (Column) o.getObject().getAllColumnRefs().get(0);
		// if (!o.getOutputName().equals(c.getAlias() + "_" + c.getName())) {
		// resultRel.renameColumn(c.toString(), o.getOutputName());
		// columns.add(o.getOutputName());
		// } else {
		// columns.add(c.toString());
		// }
		// }
		// }
		//
		// // remove unecessary columns
		// resultRel.eliminteRedundantAttributes(columns);
		//
		// // TODO: fix nodeInfo
		// ni.setNumberOfTuples(child.getNodeInfo().getNumberOfTuples());
		// // ni.setTupleLength(child.getNodeInfo().getTupleLength());
		// ni.setTupleLength(resultRel.getTupleLength());
		// // System.out.println("is this correct?");
		// ni.setResultRel(resultRel);
		// n.setNodeInfo(ni);
	}

	public void estimateUnion(Node n) {
		Node unionOp = n.getChildAt(0);
		List<Node> children = unionOp.getChildren();
		double numOfTuples = 0;
		double tupleLength = children.get(0).getNodeInfo().getTupleLength();

		for (Node cn : children) {
			numOfTuples += cn.getNodeInfo().getNumberOfTuples();
		}
		NodeInfo ni = new NodeInfo();
		// TODO: fix nodeInfo
		ni.setResultRel(children.get(0).getNodeInfo().getResultRel());
		ni.setNumberOfTuples(numOfTuples);
		ni.setTupleLength(tupleLength);
		n.setNodeInfo(ni);
	}

	public void estimateBase(Node n) {
		NodeInfo pi = new NodeInfo();
		Table t = (Table) n.getObject();

		RelInfo rel = this.schema.getTableIndex().get(t.getName());
		// RelInfo rel = this.planInfo.get(n.getHashId()).getResultRel();

		// System.out.println(rel);
		RelInfo resultRel = new RelInfo(rel, t.getAlias(), false);
		/*
		 * Map<String, AttrInfo> aliasAtts=new HashMap<String, AttrInfo>();
		 * for(String colname:resultRel.getAttrIndex().keySet()){
		 * aliasAtts.put(tableAlias+"."+colname,
		 * resultRel.getAttrIndex().get(colname)); }
		 * resultRel.setAttrIndex(aliasAtts);
		 */

		// TODO: fix nodeInfo
		pi.setNumberOfTuples(rel.getNumberOfTuples());
		pi.setTupleLength(rel.getTupleLength());
		pi.setResultRel(resultRel);
		n.setNodeInfo(pi);
	}

	/* private-util methods */
	public static double hashString(String str) {
		if (str == null)
			return 0;
		double hashStringVal = 0.0;
		if (str.length() >= HASH_STRING_CHARS) {
			char[] hashChars = new char[HASH_STRING_CHARS];

			for (int i = 0; i < HASH_STRING_CHARS; i++) {
				hashChars[i] = str.charAt(i);
			}

			for (int i = 0; i < HASH_STRING_CHARS; i++) {
				hashStringVal += (double) ((int) hashChars[i])
						* Math.pow((double) HASH_STRING_BASE, (double) (HASH_STRING_CHARS - i));
			}
			return hashStringVal;
		}

		else {
			char[] hashChars = new char[str.length()];

			for (int i = 0; i < str.length(); i++)
				hashChars[i] = str.charAt(i);

			for (int i = 0; i < str.length(); i++) {
				hashStringVal += (double) ((int) hashChars[i])
						* Math.pow((double) HASH_STRING_BASE, (double) (HASH_STRING_CHARS - i));
			}

			return hashStringVal;
		}

	}

	public void setRdfTypeTable(int rdfTypeTable) {
		this.rdfTypeTable = rdfTypeTable;
	}

	public NodeInfo estimateJoin(NodeInfo left, NodeInfo right, NonUnaryWhereCondition nuwc) {
		NodeInfo ni = new NodeInfo();
		Column l = nuwc.getLeftOp().getAllColumnRefs().get(0);
		Column r = nuwc.getRightOp().getAllColumnRefs().get(0);
		if (!(nuwc.getLeftOp() instanceof Column)) {
			log.debug("Operand not Column. Selectivity estimation may not be accurate:" + nuwc.getLeftOp());
		}
		if (!(nuwc.getRightOp() instanceof Column)) {
			log.debug("Operand not Column. Selectivity estimation may not be accurate:" + nuwc.getRightOp());
		}

		// RelInfo lRel = this.schema.getTableIndex().get(l.tableAlias);
		// RelInfo rRel = this.schema.getTableIndex().get(r.tableAlias);
		RelInfo lRel = left.getResultRel();
		RelInfo rRel = right.getResultRel();

		RelInfo resultRel = new RelInfo(lRel);
		RelInfo newR = new RelInfo(rRel);

		AttrInfo leftAttr = resultRel.getAttrIndex().get(l);
		AttrInfo rightAttr = newR.getAttrIndex().get(r);
		List<Integer> tables = new ArrayList<Integer>(2);
		int baseJoinSize = -1;
		boolean smallIsSubject;
		boolean largeIsSubject;
		double estimatedSize=-1;
		boolean leftIsSmall=lRel.getNumberOfTuples()<rRel.getNumberOfTuples();
		if (leftAttr.getAttrName().getAlias() < rightAttr.getAttrName().getAlias()) {
			tables.add(leftAttr.getAttrName().getAlias());
			tables.add(rightAttr.getAttrName().getAlias());
			smallIsSubject = leftAttr.getAttrName().getColumnName();
			largeIsSubject = rightAttr.getAttrName().getColumnName();
		} else {
			tables.add(rightAttr.getAttrName().getAlias());
			tables.add(leftAttr.getAttrName().getAlias());
			
			largeIsSubject = leftAttr.getAttrName().getColumnName();
			smallIsSubject = rightAttr.getAttrName().getColumnName();
		}
		if (schema.getCards() != null) {
			int baseTableSizes[] = schema.getCards().getSizesForTables(tables);
			if (baseTableSizes != null) {
				if (smallIsSubject) {
					if (largeIsSubject) {
						baseJoinSize = baseTableSizes[0];
					} else {
						baseJoinSize = baseTableSizes[1];
					}
				} else {
					if (largeIsSubject) {
						baseJoinSize = baseTableSizes[2];
					} else {
						baseJoinSize = baseTableSizes[3];
					}
				}
			}
		}
		if(baseJoinSize>-1){
			double baseSmall=this.schema.getTableIndex().get(tables.get(0)).getNumberOfTuples();
			double baseLarge=this.schema.getTableIndex().get(tables.get(1)).getNumberOfTuples();
			double factorSmall=1.0;
			double factorLarge=1.0;
			if(leftIsSmall){
				factorSmall=left.getNumberOfTuples()/baseSmall;
				factorLarge=right.getNumberOfTuples()/baseLarge;
				
			}
			else{
				factorSmall=right.getNumberOfTuples()/baseSmall;
				factorLarge=left.getNumberOfTuples()/baseLarge;
			}
			estimatedSize=factorSmall*factorLarge*baseJoinSize;
		}

		Histogram resultHistogram = leftAttr.getHistogram();
		if (newR.getNumberOfTuples() < 0.5 || lRel.getNumberOfTuples() < 0.5) {
			resultHistogram.convertToTransparentHistogram();
		} else {
			if (nuwc.getOperator().contains(">") || nuwc.getOperator().contains("<")) {
				resultHistogram.rangejoin(newR.getAttrIndex().get(r).getHistogram());
			} else {
				resultHistogram.join(rightAttr.getHistogram(), estimatedSize);
			}

		}

		// lRel.getAttrIndex().get(l.columnName).getHistogram().join(rRel.getAttrIndex().get(r.columnName).getHistogram());

		// put all the right's RelInfo AttrInfos to the left one
		resultRel.getAttrIndex().putAll(newR.getAttrIndex());

		// adjust RelInfo's histograms based on the resulting histogram
		resultRel.adjustRelation(l, resultHistogram);

		// fix alias mappings to RelInfo. The joining aliases must point to the
		// same RelInfo after the join operation
		// schema.getTableIndex().put(l.toString(), resultRel);
		// schema.getTableIndex().put(r.toString(), resultRel);

		// adding necessary equivalent hashing attribures

		// TODO: fix nodeInfo
		ni.setNumberOfTuples(resultRel.getNumberOfTuples());
		ni.setTupleLength(resultRel.getTupleLength());
		ni.setResultRel(resultRel);
		return ni;

	}

	public NodeInfo estimateFilterJoin(NodeInfo resultInfo, NonUnaryWhereCondition nuwc) {
		NodeInfo ni = new NodeInfo();
		Column l = nuwc.getLeftOp().getAllColumnRefs().get(0);
		Column r = nuwc.getRightOp().getAllColumnRefs().get(0);
		if (!(nuwc.getLeftOp() instanceof Column)) {
			log.debug("Operand not Column. Selectivity estimation may not be accurate:" + nuwc.getLeftOp());
		}
		if (!(nuwc.getRightOp() instanceof Column)) {
			log.debug("Operand not Column. Selectivity estimation may not be accurate:" + nuwc.getRightOp());
		}
		// String equals = nuwc.getOperator();

		// RelInfo lRel = this.schema.getTableIndex().get(l.tableAlias);
		// RelInfo rRel = this.schema.getTableIndex().get(r.tableAlias);
		RelInfo lRel = resultInfo.getResultRel();
		RelInfo rRel = resultInfo.getResultRel();

		RelInfo resultRel = new RelInfo(lRel);

		Histogram resultHistogram = resultRel.getAttrIndex().get(l).getHistogram();

		if (rRel.getNumberOfTuples() < 0.5 || lRel.getNumberOfTuples() < 0.5) {
			resultHistogram.convertToTransparentHistogram();
		} else {
			resultHistogram.filterjoin(rRel.getAttrIndex().get(r).getHistogram());
		}

		// resultHistogram.filterjoin(rRel.getAttrIndex().get(r.getName()).getHistogram());

		// lRel.getAttrIndex().get(l.columnName).getHistogram().join(rRel.getAttrIndex().get(r.columnName).getHistogram());

		// put all the right's RelInfo AttrInfos to the left one
		resultRel.getAttrIndex().putAll(rRel.getAttrIndex());

		// adjust RelInfo's histograms based on the resulting histogram
		resultRel.adjustRelation(l, resultHistogram);

		// fix alias mappings to RelInfo. The joining aliases must point to the
		// same RelInfo after the join operation
		// schema.getTableIndex().put(l.toString(), resultRel);
		// schema.getTableIndex().put(r.toString(), resultRel);

		// adding necessary equivalent hashing attribures

		// TODO: fix nodeInfo
		ni.setNumberOfTuples(resultRel.getNumberOfTuples());
		ni.setTupleLength(resultRel.getTupleLength());
		ni.setResultRel(resultRel);
		return ni;
	}

	

}

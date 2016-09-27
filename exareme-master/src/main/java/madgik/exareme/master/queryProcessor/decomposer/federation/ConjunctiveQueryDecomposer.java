/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package madgik.exareme.master.queryProcessor.decomposer.federation;

import madgik.exareme.master.queryProcessor.decomposer.DecomposerUtils;
import madgik.exareme.master.queryProcessor.decomposer.dag.Node;
import madgik.exareme.master.queryProcessor.decomposer.dag.NodeHashValues;
import madgik.exareme.master.queryProcessor.decomposer.query.*;
import madgik.exareme.master.queryProcessor.decomposer.util.Util;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

//import di.madgik.statistics.planner.JoinExecutionAdvice;
//import di.madgik.statistics.planner.StarPlanner;
import org.apache.log4j.Logger;
import org.jfree.util.Log;

import com.google.common.hash.HashCode;

/**
 * @author dimitris
 */
public class ConjunctiveQueryDecomposer {

	private SQLQuery initialQuery;
	private List<NonUnaryWhereCondition> remainingWhereConditions;
	private ColumnsToTableNames<Node> c2n;
	private boolean checkForRangeJoins = true;
	HashSet<Join> lj;
	private static final boolean useGreedy = DecomposerUtils.USE_GREEDY;
	private static int counter = 0;
	private static final Logger log = Logger.getLogger(ConjunctiveQueryDecomposer.class);
	private List<NonUnaryWhereCondition> rangeJoins;

	public ConjunctiveQueryDecomposer(SQLQuery initial, boolean centralized, boolean addRedundantIsNotNull) {
		this.initialQuery = initial;
		this.lj = new HashSet<Join>();
		this.remainingWhereConditions = new ArrayList<NonUnaryWhereCondition>();
		/*
		 * for (Table t : initialQuery.getInputTables()) { if (t.isFederated())
		 * { this.initialQuery.setFederated(true); if
		 * (!dbs.contains(t.getDBName())) { dbs.add(t.getDBName()); } } }
		 */
		for (NonUnaryWhereCondition bwc : initialQuery.getBinaryWhereConditions()) {
			this.remainingWhereConditions.add(bwc);
		}
		if (addRedundantIsNotNull) {
			for (int i = 0; i < this.initialQuery.getUnaryWhereConditions().size(); i++) {
				UnaryWhereCondition uwc = this.initialQuery.getUnaryWhereConditions().get(i);
				if (uwc.getNot() && uwc.getType() == UnaryWhereCondition.IS_NULL) {
					Column c = uwc.getAllColumnRefs().get(0);
					for (NonUnaryWhereCondition nuwc : this.initialQuery.getBinaryWhereConditions()) {
						if (nuwc.getOperator().equals("=")) {
							Column other = null;
							if (nuwc.getLeftOp().equals(c) && nuwc.getRightOp() instanceof Column) {
								other = (Column) nuwc.getRightOp();
							}
							if (nuwc.getRightOp().equals(c) && nuwc.getLeftOp() instanceof Column) {
								other = (Column) nuwc.getLeftOp();
							}
							if (other != null) {
								try {
									UnaryWhereCondition toAdd = new UnaryWhereCondition(UnaryWhereCondition.IS_NULL,
											other.clone(), true);
									if (!this.initialQuery.getUnaryWhereConditions().contains(toAdd)) {
										this.initialQuery.getUnaryWhereConditions().add(toAdd);
									}
								} catch (CloneNotSupportedException ex) {
									log.error(ex.getMessage());
								}
							}
						}
					}
				}
			}
		}
		counter++;
	}

	public Node addCQToDAG(Node root, NodeHashValues hashes) throws SQLException {

		// columnsToSubqueries tracks from which temporary table we take each
		// column of the initial query
		c2n = new ColumnsToTableNames<Node>();

		

		if (initialQuery.getJoinNode() != null) {
			for(String alias:initialQuery.getJoinNode().getDescendantBaseTables()){
				c2n.addTable(alias, initialQuery.getJoinNode());
			}
			
			for (int i = 0; i < initialQuery.getInputTables().size(); i++) {
				if (initialQuery.getJoinNode().getDescendantBaseTables()
						.contains(initialQuery.getInputTables().get(i).getAlias())) {
					initialQuery.getInputTables().remove(i);
					i--;
				}
			}
			//Node tempParent=null;
			if(!initialQuery.getInputTables().isEmpty()){
				log.debug("left join with more tables in the same query");
			for (Table t : this.initialQuery.getInputTables()) {
				makeNodeForTable(t, c2n, hashes);

			}
			Node tempParent=addRemainingJoins(c2n, hashes, null, root, false);
			root.addChild(tempParent);
			return tempParent; }
			else{
				Node tempParent = makeNodeFinal(initialQuery.getJoinNode(), hashes);
				
				if (useGreedy) {
					tempParent.addUnionToDesc(counter);
				}
				// String a=tempParent.dotPrint();
				Selection s = new Selection();
				if (!initialQuery.getBinaryWhereConditions().isEmpty()) {

					for (NonUnaryWhereCondition nuwc : initialQuery.getBinaryWhereConditions()) {
						s.addOperand(nuwc);
					}
				}
				if (!initialQuery.getUnaryWhereConditions().isEmpty()) {

					for (UnaryWhereCondition uwc : initialQuery.getUnaryWhereConditions()) {
						s.addOperand(uwc);
					}
				}
				if(!s.getOperands().isEmpty()){
					Node selNode=new Node(Node.AND, Node.SELECT);
					selNode.setObject(s);
					selNode.addChild(tempParent);
					selNode.addAllDescendantBaseTables(tempParent.getDescendantBaseTables());
					if (!hashes.containsKey(selNode.getHashId())) {

						// table.setHashID(Objects.hash(t.getName()));
						// table.setIsBaseTable(true);
						hashes.put(selNode.getHashId(), selNode);
					} else {
						selNode = hashes.get(selNode.getHashId());
					}
					Node table = new Node(Node.OR);
					Table t = new Table("table" + Util.createUniqueId(), null);
					table.setObject(t);
					table.addChild(selNode);
					// table.setPartitionRecord(groupBy.getPartitionRecord());
					// table.setLastPartition(groupBy.getLastPartition());

					// groupBy.setPartitionRecord(n.getPartitionRecord());
					// groupBy.setPartitionRecord(n.getPartitionRecord());
					if (!hashes.containsKey(table.getHashId())) {
						hashes.put(table.getHashId(), table);
						table.addAllDescendantBaseTables(selNode.getDescendantBaseTables());
					} else {
						table = hashes.get(table.getHashId());
					}
					tempParent = table;
				}
				root.addChild(tempParent);
				return tempParent;
			}
			/*makeNodeFinal(initialQuery.getJoinNode(), hashes);

			if (useGreedy) {
			tempParent.addUnionToDesc(counter);
			}
			Selection s = new Selection();
			if (!initialQuery.getBinaryWhereConditions().isEmpty()) {

				for (NonUnaryWhereCondition nuwc : initialQuery.getBinaryWhereConditions()) {
					s.addOperand(nuwc);
				}
			}
			if (!initialQuery.getUnaryWhereConditions().isEmpty()) {

				for (UnaryWhereCondition uwc : initialQuery.getUnaryWhereConditions()) {
					s.addOperand(uwc);
				}
			}
			if (!s.getOperands().isEmpty()) {
				Node selNode = new Node(Node.AND, Node.SELECT);
				selNode.setObject(s);
				selNode.addChild(tempParent);
				selNode.addAllDescendantBaseTables(tempParent.getDescendantBaseTables());
				if (!hashes.containsKey(selNode.getHashId())) {

					// table.setHashID(Objects.hash(t.getName()));
					// table.setIsBaseTable(true);
					hashes.put(selNode.getHashId(), selNode);
				} else {
					selNode = hashes.get(selNode.getHashId());
				}
				Node table = new Node(Node.OR);
				Table t = new Table("table" + Util.createUniqueId(), null);
				table.setObject(t);
				table.addChild(selNode);
				// table.setPartitionRecord(groupBy.getPartitionRecord());
				// table.setLastPartition(groupBy.getLastPartition());

				// groupBy.setPartitionRecord(n.getPartitionRecord());
				// groupBy.setPartitionRecord(n.getPartitionRecord());
				if (!hashes.containsKey(table.getHashId())) {
					hashes.put(table.getHashId(), table);
					table.addAllDescendantBaseTables(selNode.getDescendantBaseTables());
				} else {
					table = hashes.get(table.getHashId());
				}
				tempParent = table;
			}
			root.addChild(tempParent);
			return tempParent;*/

		}

		// else we have only one DB, for each table make a subquery
		// else {
		Node last = null;

		boolean checkToRemoveReduntantJoins = false;
		if (checkToRemoveReduntantJoins && root.getOpCode() == Node.UNION) {
			Map<String, Boolean> tbleHashOnlyOneCol = new HashMap<String, Boolean>();
			for (Column c : initialQuery.getAllColumns()) {
				if (tbleHashOnlyOneCol.containsKey(c.getAlias())) {
					tbleHashOnlyOneCol.put(c.getAlias(), false);
				} else {
					tbleHashOnlyOneCol.put(c.getAlias(), true);
				}
			}
			for (String s : tbleHashOnlyOneCol.keySet()) {
				if (tbleHashOnlyOneCol.get(s)) {
					for (NonUnaryWhereCondition join : initialQuery.getBinaryWhereConditions()) {
						if (join.getLeftOp() instanceof Column && join.getRightOp() instanceof Column
								&& join.getOperator().equals("=")) {
							Column left = (Column) join.getLeftOp();
							Column right = (Column) join.getRightOp();
							if (left.getName().equals(right.getName())) {
								if (left.getAlias().equals(s)) {

								}
								if (right.getAlias().equals(s)) {

								}
							}
						}
					}
				}
			}
		}

		for (Table t : this.initialQuery.getInputTables()) {
			last = makeNodeForTable(t, c2n, hashes);

		}

		for (SQLQuery nested : this.initialQuery.getNestedSelectSubqueries().keySet()) {
			String alias = this.initialQuery.getNestedSubqueryAlias(nested);
			Node table = nested.getNestedNode();
			last = table;
			ArrayList<String> dummy = new ArrayList<String>();
			dummy.add(alias);
			SQLQuery s = createSubqueriesForTables(dummy, "");

			Node selection = new Node(Node.AND, Node.SELECT);
			Selection sel = new Selection();
			for (UnaryWhereCondition uwc : s.getUnaryWhereConditions()) {

				sel.addOperand(uwc);
			}
			for (NonUnaryWhereCondition bwc : s.getBinaryWhereConditions()) {

				sel.addOperand(bwc);
			}

			if (sel.getOperands().size() > 0) {

				selection.addChild(table);
				// selection.setPartitionRecord(table.getPartitionRecord());
				// selection.setLastPartition(table.getLastPartition());
				selection.setObject(sel);
				if (!hashes.containsKey(selection.getHashId())) {
					hashes.put(selection.getHashId(), selection);
					selection.addDescendantBaseTable(alias);
					// selection.addChild(table);

				} else {
					selection = hashes.get(selection.getHashId());
				}

				Node selTable = new Node(Node.OR);

				Table selt = new Table("table" + Util.createUniqueId(), null);
				selTable.setObject(selt);
				selTable.addChild(selection);
				selTable.setPartitionRecord(selection.getPartitionRecord());
				// selTable.setLastPartition(selection.getLastPartition());
				// selTable.setIsCentralised(table.isCentralised());
				// selTable.setPartitionedOn(table.isPartitionedOn());
				if (!hashes.containsKey(selTable.getHashId())) {
					selTable.addDescendantBaseTable(alias);
					hashes.put(selTable.getHashId(), selTable);
					// selection.addChild(table);

				} else {
					selTable = hashes.get(selTable.getHashId());
				}
				last = selTable;

				for (Column cl : s.getAllColumns()) {
					c2n.putColumnInTable(cl, selTable);
					// add projections

				}
			} else {
				for (Column cl : s.getAllColumns()) {
					c2n.putColumnInTable(cl, table);
					// add projections

				}
			}
		}

		List<Set<String>> joinSets = new ArrayList<Set<String>>();
		for (Table t : this.getInputTables()) {
			Set<String> tableSet = new HashSet<String>();
			tableSet.add(t.getAlias());
			joinSets.add(tableSet);
		}
		for (String n : this.initialQuery.getNestedSelectSubqueries().values()) {
			Set<String> tableSet = new HashSet<String>();
			tableSet.add(n);
			joinSets.add(tableSet);
		}
		if (remainingWhereConditions.isEmpty()) {

			if (joinSets.size() > 1) {
				NonUnaryWhereCondition toAdd = addConditionsFromConstants();
				if (toAdd == null) {
					log.error("Input query contains cartesian product. Currently not supported");

				} else {
					this.initialQuery.getBinaryWhereConditions().add(toAdd);
					remainingWhereConditions.add(toAdd);
				}
			} else {
				if (last == null) {
					return root;
				}
				Node tempParent = makeNodeFinal(last, hashes);
				root.addChild(tempParent);
				if (useGreedy) {
					tempParent.addUnionToDesc(counter);
				}
				return tempParent;
			}

		}

		if (this.checkForRangeJoins) {
			Map<Set<String>, Set<NonUnaryWhereCondition>> joinsForTablesWithInequality = new HashMap<Set<String>, Set<NonUnaryWhereCondition>>();
			for (NonUnaryWhereCondition bwc : this.remainingWhereConditions) {
				if (bwc.getOperator().contains(">") || bwc.getOperator().contains("<")) {
					Set<String> ts = new HashSet<String>(2);
					ts.add(bwc.getLeftOp().getAllColumnRefs().get(0).getAlias());
					ts.add(bwc.getRightOp().getAllColumnRefs().get(0).getAlias());
					if (!joinsForTablesWithInequality.containsKey(ts)) {
						joinsForTablesWithInequality.put(ts, new HashSet<NonUnaryWhereCondition>());
					}
					joinsForTablesWithInequality.get(ts).add(bwc);
				}
			}
			for (Set<NonUnaryWhereCondition> filters : joinsForTablesWithInequality.values()) {
				for (NonUnaryWhereCondition f : filters) {
					remainingWhereConditions.remove(f);
				}
			}
			if (!joinsForTablesWithInequality.isEmpty()) {
				for (NonUnaryWhereCondition bwc : this.remainingWhereConditions) {
					if (bwc.getOperator().equals("=")) {
						Set<String> ts = new HashSet<String>(2);
						ts.add(bwc.getLeftOp().getAllColumnRefs().get(0).getAlias());
						ts.add(bwc.getRightOp().getAllColumnRefs().get(0).getAlias());
						if (joinsForTablesWithInequality.containsKey(ts)) {
							bwc.createFilterJoins();
							for (NonUnaryWhereCondition rangeJoinsForTable : joinsForTablesWithInequality.get(ts)) {
								bwc.addFilterJoin(rangeJoinsForTable);
							}
							joinsForTablesWithInequality.remove(ts);
						}

					}
				}
			}
			if (!joinsForTablesWithInequality.isEmpty()) {
				rangeJoins=new ArrayList<NonUnaryWhereCondition>();
				for (Set<NonUnaryWhereCondition> filters : joinsForTablesWithInequality.values()) {
					for (NonUnaryWhereCondition f : filters) {
						remainingWhereConditions.add(f);
						rangeJoins.add(f);
					}
				}

			}
		}

		return addRemainingJoins(c2n, hashes, joinSets, root, true);
	}

	private Node addRemainingJoins(ColumnsToTableNames<Node> c2n2, NodeHashValues hashes, List<Set<String>> joinSets, Node root, boolean useJoinSets) throws SQLException {
		while (!this.remainingWhereConditions.isEmpty()) {
			NonUnaryWhereCondition bwc = this.remainingWhereConditions.get(0);
			if(rangeJoins!=null){
				for(NonUnaryWhereCondition range:rangeJoins){
					//make sure that all columns for each operand come from the same node
					boolean allColumnComeFromSameNode=true;
				Node lchild = c2n.getTablenameForColumn(range.getLeftOp().getAllColumnRefs().get(0));
				for(int i=1;i<range.getLeftOp().getAllColumnRefs().size();i++){
					if(!c2n.getTablenameForColumn(range.getLeftOp().getAllColumnRefs().get(i)).getObject().equals(lchild.getObject())){
						allColumnComeFromSameNode=false;
						break;
					}
				}
				if(!allColumnComeFromSameNode){
					continue;
				}
				Node rchild = c2n.getTablenameForColumn(range.getRightOp().getAllColumnRefs().get(0));
				for(int i=1;i<range.getRightOp().getAllColumnRefs().size();i++){
					//Node llll=c2n.getTablenameForColumn(range.getRightOp().getAllColumnRefs().get(i));
					if(!c2n.getTablenameForColumn(range.getRightOp().getAllColumnRefs().get(i)).getObject().equals(rchild.getObject())){
						allColumnComeFromSameNode=false;
						break;
					}
				}
				if(!allColumnComeFromSameNode)
				{
					continue;
				}
				rangeJoins.remove(range);
				bwc=range;
				break;
				}
				
			}
			
			if(useJoinSets){
				mergeJoinSets(joinSets, bwc);
			}
			Node join = new Node(Node.AND, Node.JOIN);
			join.setObject(bwc);

			if (bwc.getLeftOp() instanceof Column && bwc.getRightOp() instanceof Column) {
				Node lchild = c2n.getTablenameForColumn( bwc.getLeftOp().getAllColumnRefs().get(0));
				join.addChild(lchild);
				join.addAllDescendantBaseTables(lchild.getDescendantBaseTables());

				Node rchild = c2n.getTablenameForColumn((Column) bwc.getRightOp().getAllColumnRefs().get(0));
				join.addChild(rchild);
				join.addAllDescendantBaseTables(rchild.getDescendantBaseTables());

			} else {

				log.error(bwc.toString() + ":operand not Column");
				Node lchild = c2n.getTablenameForColumn( bwc.getLeftOp().getAllColumnRefs().get(0));
				join.addChild(lchild);
				join.addAllDescendantBaseTables(lchild.getDescendantBaseTables());

				Node rchild = c2n.getTablenameForColumn((Column) bwc.getRightOp().getAllColumnRefs().get(0));
				join.addChild(rchild);
				join.addAllDescendantBaseTables(rchild.getDescendantBaseTables());

			}
			if (bwc.getOperator().contains(">") || bwc.getOperator().contains(">")) {
				if (join.getChildren().size() == 2) {
					log.warn("Query contains range join:" + remainingWhereConditions.toString() + ". "
							+ "Switching to centralized execution.");
					hashes.setContainsRangeJoin(true);
				}
			}

			HashCode hc = join.getHashId();
			if (!hashes.containsKey(hc)) {
				hashes.put(join.getHashId(), join);
			} else {
				join.removeAllChildren();
				join = hashes.get(hc);
			}

			Node table = new Node(Node.OR);
			Table t = new Table("table" + Util.createUniqueId(), null);
			table.setObject(t);
			table.addChild(join);
			// table.setIsCentralised(tableIsCentralised);
			hc = table.getHashId();
			if (!hashes.containsKey(hc)) {
				hashes.put(table.getHashId(), table);
				table.addAllDescendantBaseTables(join.getDescendantBaseTables());
			} else {
				table.removeAllChildren();
				table = hashes.get(hc);
			}

			// change columns according to columnsToSubqueries
			// ConcurrentHashMap<Column, Column> changePairs = new
			// ConcurrentHashMap();

			// for (Node toChange : join.getChildren()) {
			c2n.changeColumns(c2n.getTablenameForColumn(bwc.getLeftOp().getAllColumnRefs().get(0)), table);
			c2n.changeColumns(c2n.getTablenameForColumn( bwc.getRightOp().getAllColumnRefs().get(0)), table);
			// }

			if (remainingWhereConditions.size() == 1) {
				if (useJoinSets&&joinSets.size() > 1) {
					NonUnaryWhereCondition toAdd = addConditionsFromConstants();
					if (toAdd == null) {
						log.error("Input query contains cartesian product. Currently not supported");
						throw new SQLException("Input query contains cartesian product. Currently not supported");

					} else {
						this.initialQuery.getBinaryWhereConditions().add(toAdd);
						remainingWhereConditions.add(toAdd);
					}
				} else {
					Node tempParent = makeNodeFinal(table, hashes);
					root.addChild(tempParent);
					if (useGreedy) {
						tempParent.addUnionToDesc(counter);
					}
					return tempParent;
				}

			}

			this.remainingWhereConditions.remove(bwc);

		}

		return null;
		// return result;
	}

	private Node makeNodeForTable(Table t, ColumnsToTableNames<Node> c2n2, NodeHashValues hashes) {
		Node table = new Node(Node.OR);
		table.addDescendantBaseTable(t.getAlias());

		table.setObject(t);
		if (!hashes.containsKey(table.getHashId())) {

			// table.setHashID(Objects.hash(t.getName()));
			// table.setIsBaseTable(true);
			hashes.put(table.getHashId(), table);
		} else {
			table = hashes.get(table.getHashId());
		}
		Node last = table;
		ArrayList<String> dummy = new ArrayList<String>();
		dummy.add(t.getAlias());
		SQLQuery s = createSubqueriesForTables(dummy, "");

		Node selection = new Node(Node.AND, Node.SELECT);
		Selection sel = new Selection();
		for (UnaryWhereCondition uwc : s.getUnaryWhereConditions()) {

			sel.addOperand(uwc);
		}
		for (NonUnaryWhereCondition bwc : s.getBinaryWhereConditions()) {

			sel.addOperand(bwc);
		}
		if (sel.getOperands().size() > 0) {

			selection.addChild(table);
			// selection.setPartitionRecord(table.getPartitionRecord());
			// selection.setLastPartition(table.getLastPartition());
			selection.setObject(sel);
			if (!hashes.containsKey(selection.getHashId())) {
				hashes.put(selection.getHashId(), selection);
				selection.addDescendantBaseTable(t.getAlias());
				// selection.addChild(table);

			} else {
				selection = hashes.get(selection.getHashId());
			}

			Node selTable = new Node(Node.OR);

			Table selt = new Table("table" + Util.createUniqueId(), null);
			selTable.setObject(selt);
			selTable.addChild(selection);

			if (!hashes.containsKey(selTable.getHashId())) {
				selTable.addDescendantBaseTable(t.getAlias());
				hashes.put(selTable.getHashId(), selTable);
				// selection.addChild(table);

			} else {
				selTable = hashes.get(selTable.getHashId());

			}
			last = selTable;

			for (Column cl : s.getAllColumns()) {
				c2n.putColumnInTable(cl, selTable);
				// add projections

			}
		} else {
			for (Column cl : s.getAllColumns()) {
				c2n.putColumnInTable(cl, table);
				// add projections

			}
		}
		return last;
	}

	private NonUnaryWhereCondition addConditionsFromConstants() {
		NonUnaryWhereCondition result = null;
		for (int i = 0; i < this.initialQuery.getBinaryWhereConditions().size(); i++) {
			NonUnaryWhereCondition nuwc = this.initialQuery.getBinaryWhereConditions().get(i);
			if (nuwc.getOperator().equals("=")
					&& (nuwc.getLeftOp() instanceof Constant || nuwc.getRightOp() instanceof Constant)) {
				Constant c = nuwc.getLeftOp() instanceof Constant ? (Constant) nuwc.getLeftOp()
						: (Constant) nuwc.getRightOp();
				Operand o = nuwc.getLeftOp();
				if (o.equals(c)) {
					o = nuwc.getRightOp();
				}
				for (int j = i + 1; j < this.initialQuery.getBinaryWhereConditions().size(); j++) {
					NonUnaryWhereCondition nuwc2 = this.initialQuery.getBinaryWhereConditions().get(j);
					if (nuwc2.getOperator().equals("=")
							&& (nuwc2.getLeftOp().equals(c) || nuwc2.getRightOp().equals(c))) {
						Operand o2 = nuwc2.getLeftOp();
						if (o2.equals(c)) {
							o2 = nuwc2.getRightOp();
						}
						result = new NonUnaryWhereCondition();
						result.addOperand(o);
						result.addOperand(o2);
						result.setOperator("=");
						NonUnaryWhereCondition reverse = new NonUnaryWhereCondition();
						reverse.addOperand(o2);
						reverse.addOperand(o);
						reverse.setOperator("=");
						if (!this.initialQuery.getBinaryWhereConditions().contains(result)
								&& !this.initialQuery.getBinaryWhereConditions().contains(reverse)) {
							return result;
						}
					}
				}
			}
		}
		return null;
	}

	private void mergeJoinSets(List<Set<String>> joinSets, NonUnaryWhereCondition bwc) {
		String l = bwc.getLeftOp().getAllColumnRefs().get(0).getAlias();
		String r = bwc.getRightOp().getAllColumnRefs().get(0).getAlias();
		;
		for (int i = 0; i < joinSets.size(); i++) {
			Set<String> tables = joinSets.get(i);
			if (tables.contains(l) && tables.contains(r)) {
				return;
			} else if (tables.contains(r)) {
				for (int j = 0; j < joinSets.size(); j++) {
					Set<String> tables2 = joinSets.get(j);
					if (tables2.contains(l)) {
						for (String t : tables2) {
							tables.add(t);
						}
						joinSets.remove(tables2);
						return;
					}
				}
			} else if (tables.contains(l)) {
				for (int j = 0; j < joinSets.size(); j++) {
					Set<String> tables2 = joinSets.get(j);
					if (tables2.contains(r)) {
						for (String t : tables2) {
							tables.add(t);
						}
						joinSets.remove(tables2);
						return;
					}
				}
			}
		}
	}

	private SQLQuery createSubqueriesForTables(ArrayList<String> tablesFromDB, String dbID) {
		SQLQuery sub = new SQLQuery();

		for (Column c : initialQuery.getAllColumns()) {
			if (tablesFromDB.contains(c.getAlias())) {
				sub.addOutputColumnIfNotExists(c.getAlias(), c.getName());
			}
		}
		/*
		 * for (Output o : initialQuery.outputs) { Operand op = o.getObject();
		 * if (o.getObject() instanceof Column) { Column c = (Column)
		 * o.getObject(); if (tablesFromDB.contains(c.tableAlias)) {
		 * //sub.outputColumns.add(c);
		 * sub.addOutputColumnIfNotExists(c.tableAlias, c.columnName); } } else
		 * { //if (o.getObject() instanceof Function) { // Function f =
		 * (Function) o.getObject(); for (Column c : op.getAllColumnRefs()) { if
		 * (tablesFromDB.contains(c.tableAlias)) {
		 * sub.addOutputColumnIfNotExists(c.tableAlias, c.columnName); } } } //}
		 * 
		 * }
		 */

		for (Table t : initialQuery.getInputTables()) {
			if (tablesFromDB.contains(t.getAlias())) {
				Table t2 = new Table();
				t2.setAlias(t.getAlias());
				if (dbID != null && !dbID.equals("")) {
					if (!t2.hasDBIdRemoved()) {
						t2.setName(t.getlocalName());
						t2.setDBIdRemoved();
					}
					sub.setFederated(true);
					sub.setMadisFunctionString(DBInfoReaderDB.dbInfo.getDB(dbID).getMadisString());
				} else {
					t2.setName(t.getName());
				}
				sub.getInputTables().add(t2);
			}
		}

		for (UnaryWhereCondition uwc : initialQuery.getUnaryWhereConditions()) {
			// if filter is on table of the DB, add filter to the subquery
			if (tablesFromDB.contains(uwc.getAllColumnRefs().get(0).getAlias())) {
				sub.getUnaryWhereConditions().add(uwc);
				// add temporary column if not exists
				sub.addOutputColumnIfNotExists(uwc.getAllColumnRefs().get(0).getAlias(),
						uwc.getAllColumnRefs().get(0).getName());

			}
		}

		for (NonUnaryWhereCondition bwc : initialQuery.getBinaryWhereConditions()) {
			boolean allTablesBelong = true;

			// add temporary column if not exists
			for (Column c : bwc.getAllColumnRefs()) {

				if (tablesFromDB.contains(c.getAlias())) {
					sub.addOutputColumnIfNotExists(c.getAlias(), c.getName());

				} else {
					allTablesBelong = false;
				}
			}
			// if both left and right tables belong to DB add the join to
			// suquery
			if (allTablesBelong) {
				sub.getBinaryWhereConditions().add(bwc);
				this.remainingWhereConditions.remove(bwc);
			}
		}
		if (this.remainingWhereConditions.isEmpty()) {
			// we only have this subquery, add udf, groupby, orderby, limit and
			// rename output columns
			for (Output o : this.initialQuery.getOutputs()) {
				if (!(o.getObject() instanceof Column)) {
					sub.getOutputs().add(o);
				}
			}

			for (Column c : this.initialQuery.getGroupBy()) {
				sub.getGroupBy().add(c);
			}
			for (ColumnOrderBy c : this.initialQuery.getOrderBy()) {
				sub.getOrderBy().add(c);
			}
			sub.setLimit(this.initialQuery.getLimit());
			sub.setTemporary(false);

			sub.getOutputs().clear();
			for (Output initialOut : this.initialQuery.getOutputs()) {
				sub.getOutputs().add(initialOut);
			}
		}

		return sub;

	}

	private Node makeNodeFinal(Node n, NodeHashValues hashes) {
		Node tempParent = n;
		// boolean isCentralised = n.isCentralised();
		// Set<PartitionCols> partitioned = n.isPartitionedOn();

		if (!this.initialQuery.getGroupBy().isEmpty()) {
			if (DecomposerUtils.USE_GROUP_BY) {
				Node groupBy = new Node(Node.AND, Node.GROUPBY);
				groupBy.setObject(this.initialQuery.getGroupBy());
				Node table = new Node(Node.OR);
				Table t = new Table("table" + Util.createUniqueId(), null);
				table.setObject(t);
				table.addChild(groupBy);
				// table.setPartitionRecord(groupBy.getPartitionRecord());
				// table.setLastPartition(groupBy.getLastPartition());
				groupBy.addChild(n);
				groupBy.addAllDescendantBaseTables(n.getDescendantBaseTables());
				// groupBy.setPartitionRecord(n.getPartitionRecord());
				// groupBy.setPartitionRecord(n.getPartitionRecord());
				if (!hashes.containsKey(table.getHashId())) {
					hashes.put(table.getHashId(), table);
					table.addAllDescendantBaseTables(groupBy.getDescendantBaseTables());
				} else {
					table = hashes.get(table.getHashId());
				}
				tempParent = table;
			} else {
				log.error("GROUP BY not supported!");
			}
		}

		if (!this.initialQuery.getOrderBy().isEmpty()) {
			if (DecomposerUtils.USE_ORDER_BY) {
				Node orderBy = new Node(Node.AND, Node.ORDERBY);
				orderBy.setObject(this.initialQuery.getOrderBy());
				Node table = new Node(Node.OR);
				Table t = new Table("table" + Util.createUniqueId(), null);
				table.setObject(t);
				table.addChild(orderBy);
				// table.setPartitionRecord(orderBy.getPartitionRecord());
				// table.setLastPartition(orderBy.getLastPartition());
				orderBy.addChild(n);
				orderBy.addAllDescendantBaseTables(n.getDescendantBaseTables());
				// orderBy.setPartitionRecord(n.getPartitionRecord());
				// orderBy.setLastPartition(n.getLastPartition());
				if (!hashes.containsKey(table.getHashId())) {
					hashes.put(table.getHashId(), table);
					table.addAllDescendantBaseTables(orderBy.getDescendantBaseTables());
				} else {
					table = hashes.get(table.getHashId());
				}
				tempParent = table;
			} else {
				// log.error("ORDER BY not supported!");
			}
		}
		if (!this.initialQuery.isSelectAll()) {
			Node projection = new Node(Node.AND, Node.PROJECT);
			Projection p = new Projection();
			for (Output o : this.initialQuery.getOutputs()) {
				// c2n.putColumnInTable(c, projection);
				// add projections
				p.addOperand(o);

			}
			if (p.getOperands().size() > 0) {
				p.setDistinct(initialQuery.isOutputColumnsDinstict());
				projection.setObject(p);
				projection.addChild(tempParent);
				// projection.setPartitionRecord(tempParent.getPartitionRecord());
				// projection.setLastPartition(tempParent.getLastPartition());
				if (!hashes.containsKey(projection.getHashId())) {
					hashes.put(projection.getHashId(), projection);
					projection.addAllDescendantBaseTables(tempParent.getDescendantBaseTables());
				} else {
					projection = hashes.get(projection.getHashId());
				}
				tempParent = projection;
			}

			/*
			 * else { Node n2 = hashes.get(projection.computeHashID());
			 * n2.addChild(tempParent); tempParent = n2; }
			 */
			Node projTable = new Node(Node.OR);
			projTable.setObject(new Table("table" + Util.createUniqueId(), null));
			projTable.addChild(tempParent);

			if (!hashes.containsKey(projTable.getHashId())) {
				hashes.put(projTable.getHashId(), projTable);
				projTable.addAllDescendantBaseTables(tempParent.getDescendantBaseTables());
			} else {
				projTable = hashes.get(projTable.getHashId());
			}
			tempParent = projTable;

		}

		if (this.initialQuery.getLimit() > -1) {
			Node limit = new Node(Node.AND, Node.LIMIT);
			limit.setObject(new Integer(this.initialQuery.getLimit()));
			limit.addChild(tempParent);
			// projection.setPartitionRecord(tempParent.getPartitionRecord());
			// projection.setLastPartition(tempParent.getLastPartition());
			if (!hashes.containsKey(limit.getHashId())) {
				hashes.put(limit.getHashId(), limit);
				limit.addAllDescendantBaseTables(tempParent.getDescendantBaseTables());
			} else {
				limit = hashes.get(limit.getHashId());
			}

			// isCentralised = tempParent.isCentralised();
			// partitioned = tempParent.isPartitionedOn();
			tempParent = limit;

			Node limitTable = new Node(Node.OR);
			limitTable.setObject(new Table("table" + Util.createUniqueId(), null));
			limitTable.addChild(tempParent);

			if (!hashes.containsKey(limitTable.getHashId())) {
				hashes.put(limitTable.getHashId(), limitTable);
				limitTable.addAllDescendantBaseTables(tempParent.getDescendantBaseTables());
			} else {
				limitTable = hashes.get(limitTable.getHashId());
			}
			tempParent = limitTable;
		}

		return tempParent;

	}

	Set<Join> getJoins() {
		return lj;
	}

	List<Table> getInputTables() {
		return this.initialQuery.getInputTables();
	}
}

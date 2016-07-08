/**
 * Copyright MaDgIK Group 2010 - 2015.
 */
package madgik.exareme.master.queryProcessor.decomposer.query.visitors;

import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.parser.*;
import com.google.common.hash.HashCode;

import madgik.exareme.master.queryProcessor.decomposer.dag.Node;
import madgik.exareme.master.queryProcessor.decomposer.dag.NodeHashValues;
import madgik.exareme.master.queryProcessor.decomposer.federation.ConjunctiveQueryDecomposer;
import madgik.exareme.master.queryProcessor.decomposer.federation.NamesToAliases;
import madgik.exareme.master.queryProcessor.decomposer.query.Column;
import madgik.exareme.master.queryProcessor.decomposer.query.NonUnaryWhereCondition;
import madgik.exareme.master.queryProcessor.decomposer.query.Operand;
import madgik.exareme.master.queryProcessor.decomposer.query.Output;
import madgik.exareme.master.queryProcessor.decomposer.query.QueryUtils;
import madgik.exareme.master.queryProcessor.decomposer.query.SQLQuery;
import madgik.exareme.master.queryProcessor.decomposer.query.Table;
import madgik.exareme.master.queryProcessor.decomposer.query.UnaryWhereCondition;
import madgik.exareme.master.queryProcessor.decomposer.util.Util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

/**
 * @author heraldkllapi
 */
public class SQLQueryVisitor extends AbstractVisitor {

	private boolean stop = false;
	private NodeHashValues hashes;
	private NamesToAliases n2a;
	private Map<String, Set<String>> projectRefCols;

	public SQLQueryVisitor(SQLQuery query, NodeHashValues h) {
		super(query);
		hashes = h;
		projectRefCols = new HashMap<String, Set<String>>();
	}

	public SQLQueryVisitor(SQLQuery query, NodeHashValues h, NamesToAliases n2a) {
		super(query);
		hashes = h;
		this.n2a = n2a;
		projectRefCols = new HashMap<String, Set<String>>();
	}

	public SQLQueryVisitor(SQLQuery query, NodeHashValues h, NamesToAliases n2a, Map<String, Set<String>> refCols) {
		super(query);
		hashes = h;
		this.n2a = n2a;
		projectRefCols = refCols;
	}

	@Override
	public Visitable visit(Visitable node) throws StandardException {

		if (node instanceof JoinNode) {
			if (query.getJoinNode() == null) {
				Map<String, Integer> counts = new HashMap<String, Integer>();
				query.setJoinNode(getJoinNode((JoinNode) node, counts, new HashMap<String, String>(),
						new HashMap<String, String>()));
				// decomposeJoinNode((JoinNode) node);
				// WhereClauseVisitor whereVisitor = new
				// WhereClauseVisitor(query);
				// node.accept(whereVisitor);
			}

		}

		if (node instanceof UnionNode) {
			decomposeUnionNode((UnionNode) node);
			this.query.setHasUnionRootNode(true);
			stop = true;
		}
		if (node instanceof CursorNode) {
			CursorNode cNode = (CursorNode) node;
			if (cNode.getFetchFirstClause() != null) {
				query.setLimit((int) (Integer) ((ConstantNode) cNode.getFetchFirstClause()).getValue());
			}
			if (cNode.getResultSetNode() instanceof UnionNode) {
				// top node is a union
				UnionNode uNode = (UnionNode) cNode.getResultSetNode();

				decomposeUnionNode(uNode);
				this.query.setHasUnionRootNode(true);
				stop = true;
			}

		}
		if (node instanceof SelectNode) {
			SelectVisitor selectVis = new SelectVisitor(query);
			node.accept(selectVis);
		}
		if (node instanceof OrderByList) {
			OrderByVisitor orderVisitor = new OrderByVisitor(query);
			node.accept(orderVisitor);
		}

		if (node instanceof FromList) {
			// check if we have nested subquery (union or nested select)
			FromList fl = (FromList) node;
			for (int i = 0; i < fl.size(); i++) {
				if (fl.get(i) instanceof FromSubquery) {
					FromSubquery from = (FromSubquery) fl.get(i);
					String alias = from.getCorrelationName();
					ResultSetNode rs = from.getSubquery();
					if (rs instanceof UnionNode) {
						UnionNode uNode = (UnionNode) rs;
						decomposeUnionNode(uNode);
						this.query.setUnionAlias(alias);

					}
					if (rs instanceof SelectNode) {
						// nested select
						SelectNode nestedSelectNode = (SelectNode) rs;

						SQLQuery nestedSelectSubquery = new SQLQuery();
						// query.readDBInfo();
						SQLQueryVisitor subqueryVisitor = new SQLQueryVisitor(nestedSelectSubquery, hashes, n2a,
								projectRefCols);
						nestedSelectNode.accept(subqueryVisitor);

						this.query.addNestedSelectSubquery(nestedSelectSubquery, alias);
						// ;.nestedSelectSubquery = nestedSelectSubquery;
						// this.query.setNestedSelectSubqueryAlias(alias);
					}
				}
			}
			// if (fl.get(0) instanceof JoinNode) {
			// JoinNode jNode = (JoinNode) fl.get(0);
			// decomposeJoinNode(jNode);
			// }

		}

		// Limit
		// if ().getFetchFirstClause()
		return node;
	}

	private Node getJoinNode(JoinNode node, Map<String, Integer> counts, Map<String, String> correspondingAliases,
			Map<String, String> aliasesToTables) throws StandardException {
		Node j = new Node(Node.AND, Node.JOINKEY);
		if (node instanceof HalfOuterJoinNode) {
			HalfOuterJoinNode outer = (HalfOuterJoinNode) node;
			if (outer.isRightOuterJoin()) {
				j.setOperator(Node.RIGHTJOIN);
			} else {
				j.setOperator(Node.LEFTJOIN);
			}
		}
		ResultSetNode left = node.getLogicalLeftResultSet();
		if (left instanceof FromBaseTable) {
			FromBaseTable bt = (FromBaseTable) left;
			String originalAlias = bt.getExposedName();
			String tblName = bt.getOrigTableName().getTableName();
			String newAlias = originalAlias;
			if (counts.containsKey(tblName)) {
				counts.put(tblName, counts.get(tblName) + 1);
				newAlias = n2a.getGlobalAliasForBaseTable(tblName, counts.get(tblName));
			} else {
				counts.put(tblName, 0);
				newAlias = n2a.getGlobalAliasForBaseTable(tblName, 0);
			}
			Table t = new Table(tblName, newAlias);
			Node table = new Node(Node.OR);
			table.addDescendantBaseTable(t.getAlias());
			correspondingAliases.put(originalAlias, newAlias);
			aliasesToTables.put(originalAlias, t.getName());
			table.setObject(t);
			if (!hashes.containsKey(table.getHashId())) {

				// table.setHashID(Objects.hash(t.getName()));
				// table.setIsBaseTable(true);
				hashes.put(table.getHashId(), table);
			} else {
				table = hashes.get(table.getHashId());
			}
			j.addChild(table);
			j.addAllDescendantBaseTables(table.getDescendantBaseTables());
		} else if (left instanceof JoinNode) {
			CheckForHalfOuterJoinVisitor check = new CheckForHalfOuterJoinVisitor(new SQLQuery());
			left.accept(check);

			if (check.stopTraversal()) {
				Node n = getJoinNode((JoinNode) left, counts, correspondingAliases, aliasesToTables);
				j.addChild(n);
				j.addAllDescendantBaseTables(n.getDescendantBaseTables());
			} else {
				j=addSubquery(left, j, correspondingAliases, counts, aliasesToTables);
			}
		} else if (left instanceof FromSubquery) {
			FromSubquery fs = (FromSubquery) left;
			SQLQuery leftSubquery = new SQLQuery();
			SQLQueryVisitor v = new SQLQueryVisitor(leftSubquery, hashes, n2a, projectRefCols);
			fs.getSubquery().accept(v);
			if (leftSubquery.getInputTables().size() == 1) {
				leftSubquery.addColumnAliases();
			}
			pushFiltersToQuery(leftSubquery);
			// List<List<String>> aliases = leftSubquery.getListOfAliases(n2a,
			// true);
			// for(List<String> aliases:initialQuery.getListOfAliases(n2a)){
			// List<String> firstAliases = aliases.get(0);
			correspondingAliases.put(fs.getExposedName(), fs.getExposedName());
			aliasesToTables.put(fs.getExposedName(), fs.getExposedName());
			// leftSubquery.renameTables(firstAliases);

			Node nestedNodeOr = new Node(Node.AND, Node.NESTED);
			Node nestedNode = new Node(Node.OR);
			nestedNode.setObject(new Table("table" + Util.createUniqueId().toString(), null));
			nestedNode.addChild(nestedNodeOr);
			nestedNodeOr.setObject(fs.getExposedName());
			nestedNode.addDescendantBaseTable(fs.getExposedName());
			/*
			 * for (List<String> aliases : nested.getListOfAliases(n2a)) {
			 * nested.renameTables(aliases); ConjunctiveQueryDecomposer d = new
			 * ConjunctiveQueryDecomposer(nested, centralizedExecution,
			 * addNotNulls); d.addCQToDAG(union, hashes); }
			 */
			// for(List<String>
			// aliases:initialQuery.getListOfAliases(n2a)){
			ConjunctiveQueryDecomposer d = new ConjunctiveQueryDecomposer(leftSubquery, false, true);
			Node topSubquery=null;
			try{
			 topSubquery = d.addCQToDAG(nestedNodeOr, hashes);
			}
			catch(Exception e){
				throw new StandardException(e);
			}
			HashCode hc = nestedNode.getHashId();
			if (hashes.containsKey(hc)) {
				nestedNode.removeAllChildren();
				nestedNode = hashes.get(hc);
			} else {
				hashes.put(hc, nestedNode);
			}

			j.addChild(nestedNode);
			nestedNode.addAllDescendantBaseTables(topSubquery.getDescendantBaseTables());
			j.addAllDescendantBaseTables(topSubquery.getDescendantBaseTables());
		} else {
			System.err.println("error in join, unknown child type");
		}

		ResultSetNode right = node.getLogicalRightResultSet();
		if (right instanceof FromBaseTable) {
			FromBaseTable bt = (FromBaseTable) right;
			String originalAlias = bt.getExposedName();
			String tblName = bt.getOrigTableName().getTableName();
			String newAlias = originalAlias;
			if (counts.containsKey(tblName)) {
				counts.put(tblName, counts.get(tblName) + 1);
				newAlias = n2a.getGlobalAliasForBaseTable(tblName, counts.get(tblName));
			} else {
				counts.put(tblName, 0);
				newAlias = n2a.getGlobalAliasForBaseTable(tblName, 0);
			}
			Table t = new Table(tblName, newAlias);
			Node table = new Node(Node.OR);
			table.addDescendantBaseTable(t.getAlias());
			correspondingAliases.put(originalAlias, newAlias);
			aliasesToTables.put(originalAlias, t.getName());
			table.setObject(t);
			if (!hashes.containsKey(table.getHashId())) {

				// table.setHashID(Objects.hash(t.getName()));
				// table.setIsBaseTable(true);
				hashes.put(table.getHashId(), table);
			} else {
				table = hashes.get(table.getHashId());
			}
			j.addChild(table);
			j.addAllDescendantBaseTables(table.getDescendantBaseTables());
		} else if (right instanceof JoinNode) {
			CheckForHalfOuterJoinVisitor check = new CheckForHalfOuterJoinVisitor(new SQLQuery());
			right.accept(check);

			if (check.stopTraversal()) {
				Node n = getJoinNode((JoinNode) right, counts, correspondingAliases, aliasesToTables);
				j.addChild(n);
				j.addAllDescendantBaseTables(n.getDescendantBaseTables());
			} else {
				j=addSubquery(right, j, correspondingAliases, counts, aliasesToTables);
			}
		} else if (right instanceof FromSubquery) {
			FromSubquery fs = (FromSubquery) right;
			SQLQuery rightSubquery = new SQLQuery();
			SQLQueryVisitor v = new SQLQueryVisitor(rightSubquery, hashes, n2a, projectRefCols);
			fs.getSubquery().accept(v);
			if (rightSubquery.getInputTables().size() == 1) {
				rightSubquery.addColumnAliases();
			}
			pushFiltersToQuery(rightSubquery);
			// List<List<String>> aliases = rightSubquery.getListOfAliases(n2a,
			// true);
			// for(List<String> aliases:initialQuery.getListOfAliases(n2a)){
			// List<String> firstAliases = aliases.get(0);
			correspondingAliases.put(fs.getExposedName(), fs.getExposedName());
			aliasesToTables.put(fs.getExposedName(), fs.getExposedName());
			// rightSubquery.renameTables(firstAliases);

			Node nestedNodeOr = new Node(Node.AND, Node.NESTED);
			Node nestedNode = new Node(Node.OR);
			nestedNode.setObject(new Table("table" + Util.createUniqueId().toString(), null));
			nestedNode.addChild(nestedNodeOr);
			nestedNodeOr.setObject(fs.getExposedName());
			nestedNode.addDescendantBaseTable(fs.getExposedName());
			/*
			 * for (List<String> aliases : nested.getListOfAliases(n2a)) {
			 * nested.renameTables(aliases); ConjunctiveQueryDecomposer d = new
			 * ConjunctiveQueryDecomposer(nested, centralizedExecution,
			 * addNotNulls); d.addCQToDAG(union, hashes); }
			 */
			// for(List<String>
			// aliases:initialQuery.getListOfAliases(n2a)){
			ConjunctiveQueryDecomposer d = new ConjunctiveQueryDecomposer(rightSubquery, false, true);
			Node topSubquery=null;
			try{
			 topSubquery = d.addCQToDAG(nestedNodeOr, hashes);
			}
			catch(Exception e){
				throw new StandardException(e);
			} 

			HashCode hc = nestedNode.getHashId();
			if (hashes.containsKey(hc)) {
				nestedNode.removeAllChildren();
				nestedNode = hashes.get(hc);
			} else {
				hashes.put(hc, nestedNode);
			}

			j.addChild(nestedNode);
			nestedNode.addAllDescendantBaseTables(topSubquery.getDescendantBaseTables());
			j.addAllDescendantBaseTables(topSubquery.getDescendantBaseTables());
		} else {
			System.err.println("error in join, unknown child type");
		}
		// List<Operand> joinConditions=new ArrayList<Operand>();
		Operand op = QueryUtils.getOperandFromNode(node.getJoinClause());
		for (Column c : op.getAllColumnRefs()) {
			if (projectRefCols.containsKey(aliasesToTables.get(c.getAlias()))) {
				projectRefCols.get(aliasesToTables.get(c.getAlias())).add(c.getAlias());
			} else {
				Set<String> aliasesForTable = new HashSet<String>();
				aliasesForTable.add(c.getAlias());
				projectRefCols.put(aliasesToTables.get(c.getAlias()), aliasesForTable);
			}
			op.changeColumn(c, new Column(correspondingAliases.get(c.getAlias()), c.getColumnName()));
		}
		QueryUtils.reorderBinaryConditions(op, j.getChildAt(0).getDescendantBaseTables(),
				j.getChildAt(1).getDescendantBaseTables());
		j.setObject(op);

		Node parent = new Node(Node.OR);

		Table selt = new Table("table" + Util.createUniqueId(), null);
		parent.setObject(selt);
		parent.addChild(j);

		if (!hashes.containsKey(parent.getHashId())) {
			parent.addAllDescendantBaseTables(j.getDescendantBaseTables());
			hashes.put(parent.getHashId(), parent);
			// selection.addChild(table);

		} else {
			parent = hashes.get(parent.getHashId());

		}
		
		return parent;
	}

	private Node addSubquery(ResultSetNode node, Node j, Map<String, String> correspondingAliases, Map<String, Integer> counts, Map<String, String> aliasesToTables)
			throws StandardException {
		SQLQuery leftQ = new SQLQuery();
		leftQ.setSelectAll(true);
		LeftJoinSubVisitor vis = new LeftJoinSubVisitor(leftQ);
		node.accept(vis);
		pushFiltersToQuery(leftQ);

		Map<String, Set<String>> refCols = new HashMap<String, Set<String>>();
		leftQ.generateRefCols(refCols);
		for (String t : refCols.keySet()) {
			if (projectRefCols.containsKey(t)) {
				projectRefCols.get(t).addAll(refCols.get(t));
			} else {
				projectRefCols.put(t, refCols.get(t));
			}
		}

		if (!leftQ.getNestedSubqueries().isEmpty()) {
			for (SQLQuery nested : leftQ.getNestedSubqueries()) {
				addNestedToDAG(nested, leftQ);
				correspondingAliases.put(leftQ.getNestedSubqueryAlias(nested), leftQ.getNestedSubqueryAlias(nested));
				aliasesToTables.put(leftQ.getNestedSubqueryAlias(nested), leftQ.getNestedSubqueryAlias(nested));
			}
		}

		List<List<String>> aliases = leftQ.getListOfAliases(n2a, true, counts);
		List<String> firstAliases = aliases.get(0);
		for (int i = 0; i < leftQ.getInputTables().size(); i++) {
			correspondingAliases.put(leftQ.getInputTables().get(i).getAlias(), firstAliases.get(i));
			aliasesToTables.put(leftQ.getInputTables().get(i).getAlias(), leftQ.getInputTables().get(i).getName());
		}
		// for(List<String>
		// aliases:initialQuery.getListOfAliases(n2a)){

		leftQ.renameTables(firstAliases);
		ConjunctiveQueryDecomposer d = new ConjunctiveQueryDecomposer(leftQ, false, true);

		Node topSubquery=null;
		try{
		 topSubquery = d.addCQToDAG(j, hashes);
		}
		catch(Exception e){
			throw new StandardException(e);
		}

		HashCode hc = j.getHashId();
		if (hashes.containsKey(hc)) {
			j.removeAllChildren();
			j = hashes.get(hc);
		} else {
			for(Node c:j.getChildren()){
				j.addAllDescendantBaseTables(c.getDescendantBaseTables());
			}
			hashes.put(hc, j);
		}
		return j;
	}

	private void pushFiltersToQuery(SQLQuery leftQ) {
		for (int i = 0; i < query.getUnaryWhereConditions().size(); i++) {
			UnaryWhereCondition uwc = query.getUnaryWhereConditions().get(i);
			Column c = uwc.getAllColumnRefs().get(0);
			if (leftQ.getTableAliases().contains(c.getAlias())) {
				leftQ.addUnaryWhereCondition(uwc);
				query.getUnaryWhereConditions().remove(uwc);
				i--;
			}
			if(leftQ.getNestedSelectSubqueries().values().contains(c.getAlias())){
				leftQ.addUnaryWhereCondition(uwc);
				query.getUnaryWhereConditions().remove(uwc);
				i--;
			}
		}

		for (int i = 0; i < query.getBinaryWhereConditions().size(); i++) {
			NonUnaryWhereCondition bwc = query.getBinaryWhereConditions().get(i);
			if (bwc.getAllColumnRefs().size() == 1) {
				Column c = bwc.getAllColumnRefs().get(0);
				if (leftQ.getTableAliases().contains(c.getAlias())) {
					leftQ.addBinaryWhereCondition(bwc);
					query.getBinaryWhereConditions().remove(bwc);
					i--;
				}
				if(leftQ.getNestedSelectSubqueries().values().contains(c.getAlias())){
					leftQ.addBinaryWhereCondition(bwc);
					query.getBinaryWhereConditions().remove(bwc);
					i--;
				}
			}
		}

	}

	@Override
	public boolean skipChildren(Visitable node) {
		return FromSubquery.class.isInstance(node) || (node instanceof JoinNode && query.getJoinType() != null);
	}

	@Override
	public boolean stopTraversal() {
		return stop;
	}

	private void decomposeUnionNode(UnionNode uNode) throws StandardException {
		SQLQuery leftSubquery = new SQLQuery();
		SQLQuery rightSubquery = new SQLQuery();
		// query.readDBInfo();
		SQLQueryVisitor leftVisitor = new SQLQueryVisitor(leftSubquery, hashes, n2a, projectRefCols);
		SQLQueryVisitor rightVisitor = new SQLQueryVisitor(rightSubquery, hashes, n2a, projectRefCols);

		if (uNode.getResultColumns() != null) {
			// uNode.getResultColumns().accept(leftVisitor);
			// uNode.getResultColumns().accept(rightVisitor);
		}
		if (uNode.getLeftResultSet() != null) {
			// uNode.getLeftResultSet().treePrint();
			if (uNode.getLeftResultSet() instanceof UnionNode) {
				decomposeUnionNode((UnionNode) uNode.getLeftResultSet());
			} else if (uNode.getLeftResultSet() instanceof JoinNode) {
				SQLQueryVisitor v = new SQLQueryVisitor(leftSubquery, hashes, n2a, projectRefCols);
				uNode.getLeftResultSet().accept(v);
				this.query.getUnionqueries().add(leftSubquery);
			} else {
				uNode.getLeftResultSet().accept(leftVisitor);
				this.query.getUnionqueries().add(leftSubquery);
			}
		}
		if (uNode.getRightResultSet() != null) {
			// uNode.getLeftResultSet().treePrint();
			if (uNode.getRightResultSet() instanceof UnionNode) {
				decomposeUnionNode((UnionNode) uNode.getRightResultSet());
			} else if (uNode.getRightResultSet() instanceof JoinNode) {
				SQLQueryVisitor v = new SQLQueryVisitor(rightSubquery, hashes, n2a, projectRefCols);
				uNode.getRightResultSet().accept(v);
				this.query.getUnionqueries().add(rightSubquery);
			} else {
				uNode.getRightResultSet().accept(rightVisitor);
				this.query.getUnionqueries().add(rightSubquery);
			}

		}
		// uNode.accept(visitor);
		this.query.setUnionAll(uNode.isAll());

	}

	private void decomposeJoinNode(JoinNode jNode) throws StandardException {
		SQLQuery leftSubquery = new SQLQuery();
		SQLQuery rightSubquery = new SQLQuery();
		// query.readDBInfo();

		// if (jNode.getResultColumns() != null) {
		// uNode.getResultColumns().accept(leftVisitor);
		// uNode.getResultColumns().accept(rightVisitor);
		// }
		if (jNode.getLeftResultSet() != null) {
			// for now we only consider that the join operators are base tables
			// or nested joins
			if (jNode.getLeftResultSet() instanceof FromSubquery) {
				FromSubquery fs = (FromSubquery) jNode.getLeftResultSet();
				SQLQueryVisitor v = new SQLQueryVisitor(leftSubquery, hashes, n2a, projectRefCols);
				fs.getSubquery().accept(v);
				this.query.setLeftJoinTableAlias(fs.getCorrelationName());
				// jNode.getLeftResultSet().accept(leftVisitor);
			} else if (jNode.getLeftResultSet() instanceof JoinNode) {
				SQLQueryVisitor v = new SQLQueryVisitor(leftSubquery, hashes, n2a, projectRefCols);
				jNode.getLeftResultSet().accept(v);
				// leftSubquery.setSelectAll(true);
			} else if (jNode.getLeftResultSet() instanceof FromBaseTable) {

				FromBaseTableVisitor v = new FromBaseTableVisitor(leftSubquery);
				jNode.getLeftResultSet().accept(v);
				// leftSubquery.setSelectAll(true);
				// DO WE NEED EXTRA QUERY FOR EACH BASE TABLE????
				// FromBaseTable bt=(FromBaseTable) jNode.getLeftResultSet();
				leftSubquery.setIsBaseTable(true);
				// leftSubquery.setResultTableName(bt.getCorrelationName());
			}
			/*
			 * for now we only consider that the join operators are base tables
			 * or nested joins else { SQLQueryVisitor v=new
			 * SQLQueryVisitor(leftSubquery);
			 * jNode.getLeftResultSet().accept(v); }
			 */

			// System.out.println("Table "+query.getResultTableName()+" add left
			// join table: "+leftSubquery.getResultTableName());
			this.query.setLeftJoinTable(leftSubquery);

		}
		/*
		 * if (jNode.getRightResultSet() != null) { //
		 * uNode.getLeftResultSet().treePrint(); if (jNode.getRightResultSet()
		 * instanceof UnionNode) { decomposeUnionNode((UnionNode)
		 * jNode.getRightResultSet()); } else if (jNode.getRightResultSet()
		 * instanceof JoinNode) { decomposeJoinNode((JoinNode)
		 * jNode.getRightResultSet()); } else { if(jNode.getRightResultSet()
		 * instanceof FromSubquery){
		 * this.query.rightJoinTableAlias=((FromSubquery)jNode.getRightResultSet
		 * ()).getCorrelationName();}
		 * 
		 * jNode.getRightResultSet().accept(rightVisitor);
		 * this.query.rightJoinTable = rightSubquery; } }
		 */

		if (jNode.getRightResultSet() != null) {

			// for now we only consider that the join operators are base tables
			// or nested joins
			if (jNode.getRightResultSet() instanceof FromSubquery) {
				FromSubquery fs = (FromSubquery) jNode.getRightResultSet();
				SQLQueryVisitor v = new SQLQueryVisitor(rightSubquery, hashes, n2a, projectRefCols);
				fs.getSubquery().accept(v);
				this.query.setRightJoinTableAlias(fs.getCorrelationName());
				// jNode.getLeftResultSet().accept(leftVisitor);
			} else if (jNode.getRightResultSet() instanceof JoinNode) {
				SQLQueryVisitor v = new SQLQueryVisitor(rightSubquery, hashes, n2a, projectRefCols);
				jNode.getRightResultSet().accept(v);
				// rightSubquery.setSelectAll(true);
			} else if (jNode.getRightResultSet() instanceof FromBaseTable) {
				// FromBaseTable bt=(FromBaseTable)jNode.getRightResultSet();
				FromBaseTableVisitor v = new FromBaseTableVisitor(rightSubquery);
				jNode.getRightResultSet().accept(v);
				// rightSubquery.setSelectAll(true);
				rightSubquery.setIsBaseTable(true);
				// rightSubquery.setResultTableName(bt.getCorrelationName());
			}
			/*
			 * for now we only consider that the join operators are base tables
			 * or nested joins else { SQLQueryVisitor v=new
			 * SQLQueryVisitor(rightSubquery);
			 * jNode.getRightResultSet().accept(v); }
			 */
			// System.out.println("Table "+query.getResultTableName()+" add
			// right join table: "+rightSubquery.getResultTableName());
			this.query.setRightJoinTable(rightSubquery);

		}
		if (jNode instanceof HalfOuterJoinNode) {
			if (!((HalfOuterJoinNode) jNode).isRightOuterJoin()) {
				this.query.setJoinType("left outer join");
			} else {
				this.query.setJoinType("right outer join");
			}
		} else {
			// we only have joins and left outer joins to add condition to
			// check!
			this.query.setJoinType("join");
		}
		// uNode.accept(visitor);

	}

	public void addNestedToDAG(SQLQuery nested, SQLQuery parent) throws StandardException {
		nested.normalizeWhereConditions();

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
		 * for (List<String> aliases : nested.getListOfAliases(n2a)) {
		 * nested.renameTables(aliases); ConjunctiveQueryDecomposer d = new
		 * ConjunctiveQueryDecomposer(nested, centralizedExecution,
		 * addNotNulls); d.addCQToDAG(union, hashes); }
		 */
		// List<List<String>> aliases = nested.getListOfAliases(n2a, true);
		// for(List<String>
		// aliases:initialQuery.getListOfAliases(n2a)){
		List<String> firstAliases = new ArrayList<String>();
		for (int i = 0; i < nested.getInputTables().size(); i++) {
			firstAliases.add("nestedalias" + i);
		}
		// nested.renameTables(firstAliases);
		ConjunctiveQueryDecomposer d = new ConjunctiveQueryDecomposer(nested, false, true);
		Node topSubquery=null;
		try{
		 topSubquery = d.addCQToDAG(nestedNodeOr, hashes);
		}
		catch(Exception e){
			throw new StandardException(e);
		}
		// String u=union.dotPrint();
		/*
		 * if (addAliases) { for (int i = 1; i < aliases.size(); i++) {
		 * List<String> nextAliases = aliases.get(i);
		 * topSubquery.addChild(addAliasesToDAG(topSubquery, firstAliases,
		 * nextAliases, hashes)); } }
		 */
		HashCode hc = nestedNode.getHashId();
		if (hashes.containsKey(hc)) {
			nestedNode.removeAllChildren();
			nestedNode = hashes.get(hc);
		} else {
			hashes.put(hc, nestedNode);
		}
		nested.putNestedNode(nestedNode);
		// nestedNode.removeAllChildren();

	}

	public Map<String, Set<String>> getProjectRefCols() {
		return projectRefCols;
	}

}

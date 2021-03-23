package madgik.exareme.master.queryProcessor.sparql;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.Projection;
import org.eclipse.rdf4j.query.algebra.ProjectionElem;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.parser.ParsedQuery;

import madgik.exareme.master.queryProcessor.decomposer.dag.Node;
import madgik.exareme.master.queryProcessor.decomposer.dag.NodeHashValues;
import madgik.exareme.master.queryProcessor.decomposer.dp.DPSubLinear;
import madgik.exareme.master.queryProcessor.decomposer.dp.EquivalentColumnClass;
import madgik.exareme.master.queryProcessor.decomposer.dp.EquivalentColumnClasses;
import madgik.exareme.master.queryProcessor.decomposer.query.Column;
import madgik.exareme.master.queryProcessor.decomposer.query.Constant;
import madgik.exareme.master.queryProcessor.decomposer.query.NonUnaryWhereCondition;
import madgik.exareme.master.queryProcessor.decomposer.query.Output;
import madgik.exareme.master.queryProcessor.decomposer.query.SQLQuery;
import madgik.exareme.master.queryProcessor.decomposer.query.Selection;
import madgik.exareme.master.queryProcessor.decomposer.query.Table;

public class DagCreator {

	private ParsedQuery pq;
	private NodeHashValues hashes;
	private int alias;
	private IdFetcher fetcher;
	private List<Node> tableNodes;
	private List<Table> tables;
	JoinClassMap classes;
	SQLQuery query;
	private List<Integer> filters;
	// list to track filter on base tables, 0 no filter, 1 filter on first, 2 filter
	// on second

	public DagCreator(ParsedQuery q, int partitions, NodeHashValues hashes, IdFetcher fetcher) {
		super();
		this.pq = q;
		this.hashes = hashes;
		this.fetcher = fetcher;
		classes = new JoinClassMap();
		tableNodes = new ArrayList<Node>();
		tables = new ArrayList<Table>();
		query = new SQLQuery();
		filters = new ArrayList<Integer>();
	}

	public SQLQuery getRootNode() throws SQLException {
		Node projection = new Node(Node.AND, Node.PROJECT);
		alias = 0;
		if (pq.getTupleExpr() instanceof Projection) {
			Projection p = (Projection) pq.getTupleExpr();

			getNodeFromExpression(p.getArg());
			EquivalentColumnClasses eqClasses = new EquivalentColumnClasses();
			for (JoinClass jc : classes.getClasses()) {
				if (jc.getColumns().size() > 1) {
					EquivalentColumnClass eqClass = new EquivalentColumnClass(jc.getColumns());
					eqClasses.add(eqClass);
				}
			}
			DPSubLinear dp = new DPSubLinear(tableNodes, eqClasses);
			dp.setNse(hashes.getNse());
			int tableOrder[] = dp.getPlan();
			eqClasses.renew();
			int inserted = 0;
			for (int i = 0; i < tableOrder.length; i++) {
				query.addInputTable(tables.get(tableOrder[i]));
				eqClasses.renew();
				Set<NonUnaryWhereCondition> joins = null;
				boolean hasJoinOnlyInSecond = true;
				for (int j = 0; j < inserted; j++) {
					joins = eqClasses.getJoin(tableOrder[i] + 1, tableOrder[j] + 1);
					if (joins != null) {
						for (NonUnaryWhereCondition join : joins) {
							query.addBinaryWhereCondition(join);
							if (hasJoinOnlyInSecond && filters.get(tableOrder[i]) == 0) {
								for (Column c : join.getAllColumnRefs()) {
									if (c.getAlias() == tables.get(tableOrder[i]).getAlias() && c.getColumnName()) {
										hasJoinOnlyInSecond = false;
									}
								}
							}
						}
					}
				}
				if (i > 0) {
					if (!hasJoinOnlyInSecond) {
						tables.get(tableOrder[i]).setInverse(false);
					} else {
						if (filters.get(tableOrder[i]) != 1) {
							tables.get(tableOrder[i]).setInverse(true);
						}
					}
				} else {
					if (filters.get(tableOrder[i]) == 2) {
						tables.get(tableOrder[i]).setInverse(true);
					}
				}

				if (i == 1 && filters.get(tableOrder[0]) == 0) {
					// first table does not have filter,
					// choose if it will be inverse depending on join with 2nd table
					if (joins != null) {
						for (NonUnaryWhereCondition join : joins) {
							for (Column c : join.getAllColumnRefs()) {
								if (c.getAlias() == tables.get(tableOrder[0]).getAlias() && !c.getColumnName()) {
									tables.get(tableOrder[0]).setInverse(true);
									break;
								}
							}
						}
					}

				}

				inserted++;
			}

			// projection.addChild(top);
			// Set<Column> projected=new HashSet<Column>();

			madgik.exareme.master.queryProcessor.decomposer.query.Projection prj = new madgik.exareme.master.queryProcessor.decomposer.query.Projection();
			projection.setObject(prj);
			for (ProjectionElem pe : p.getProjectionElemList().getElements()) {
				Column proj = classes.getFirstColumn(pe.getSourceName());
				query.getOutputs().add(new Output(pe.getTargetName(), proj));
				// Column proj= new
				// Column(current.getFirstColumn(pe.getSourceName()).getAlias(),
				// pe.getSourceName());
				// projected.add(proj);
				// prj.addOperand(new Output(pe.getTargetName(), proj));
			}
			// System.out.println(projection.dotPrint(new HashSet<Node>()));
			Node root = new Node(Node.OR);
			root.addChild(projection);
			// System.out.println(query.toSQL());
			query.setEstimatedSize(dp.getEstimatedSize());
			return query;
			// Map<String, Set<Column>> eqClasses=new HashMap<String,
			// Set<Column>>();

		} else {
			throw new SQLException("Input query does not contain projection");
		}

	}

	private void getNodeForTriplePattern(StatementPattern sp, Node top) throws SQLException {
		int pred;
		boolean selection = false;
		// Node baseTable=new Node(Node.OR);
		Table predTable = null;
		filters.add(0);
		Node selNode = new Node(Node.AND, Node.SELECT);
		Selection s = new Selection();
		selNode.setObject(s);
		String subVar = "";
		// JoinClassMap result = new JoinClassMap();
		Var predicate = sp.getPredicateVar();
		Var subject = sp.getSubjectVar();
		Var object = sp.getObjectVar();
		if (!predicate.isConstant()) {
			throw new SQLException("constant predicate not supported yet");
		} else {
			pred = (int) fetcher.getIdForProperty(predicate.getValue().stringValue());
			alias++;
			predTable = new Table(pred, alias);

			// baseTable.setObject(predTable);

		}
		if (!subject.isConstant()) {
			String varString = subject.getName();

			// joinCondition.setLeftOp(tablesForVar.iterator().next());
			Column newCol = new Column(alias, true);
			classes.add(varString, newCol);
			subVar = varString;
			// joinCondition.setRightOp(newCol);
			// tablesForVar.add(newCol);
			// }
			// else{
			// Set<Column> tablesForVar=new HashSet<Column>();
			// tablesForVar.add(new Column(aliasString, "s"));
			// eqClasses.put(varString, tablesForVar);
			// }
		} else {
			createSelection(selNode, selection, subject, alias, true);
			selection = true;
		}
		if (!object.isConstant()) {

			String varString = object.getName();

			if (subVar.equals(varString)) {
				throw new SQLException("same var in subject and object not supported yet");
			}

			Column newCol = new Column(alias, false);
			// joinCondition.setRightOp(newCol);
			classes.add(varString, newCol);

		} else {
			createSelection(selNode, selection, object, alias, false);
			selection = true;
		}
		if (selection) {
			Node baseNode = new Node(Node.OR);
			baseNode.setObject(predTable);
			hashes.getNse().makeEstimationForNode(baseNode);
			// hashes.put(baseNode.computeHashIDExpand(), baseNode);
			selNode.addChild(baseNode);
			// hashes.put(selNode.computeHashIDExpand(), selNode);
			top.addChild(selNode);
		} else {
			top.setObject(predTable);

		}
		// hashes.put(top.computeHashIDExpand(), top);
		hashes.getNse().makeEstimationForNode(top);
		top.addDescendantBaseTable("alias" + alias);
		tableNodes.add(top);
		tables.add(predTable);
		// return result;
	}

	private void createSelection(Node selNode, boolean selection, Var sbjOrObj, int aliasString, boolean sOrO)
			throws SQLException {
		// Selection s=null;
		// if(selection){
		Selection s = (Selection) selNode.getObject();
		// }
		// else{
		// selNode=new Node(Node.AND, Node.SELECT);
		// s=new Selection();
		// selNode.setObject(s);
		// }
		if (sOrO) {
			filters.set(filters.size() - 1, 1);
		} else {
			filters.set(filters.size() - 1, 2);
		}
		NonUnaryWhereCondition nuwc = new NonUnaryWhereCondition();
		nuwc.setOperator("=");
		nuwc.setLeftOp(new Column(aliasString, sOrO));
		nuwc.setRightOp(new Constant(fetcher.getIdForUri(sbjOrObj.getValue().stringValue())));
		s.addOperand(nuwc);
		query.addBinaryWhereCondition(nuwc);

	}

	private void getNodeFromExpression(TupleExpr expr) throws SQLException {
		if (expr instanceof Join) {
			Join j = (Join) expr;
			getNodeFromExpression(j.getLeftArg());
			getNodeFromExpression(j.getRightArg());
		} else if (expr instanceof StatementPattern) {
			StatementPattern sp = (StatementPattern) expr;
			Node top = new Node(Node.OR);
			getNodeForTriplePattern(sp, top);
		} else {
			throw new SQLException("Only basic graph patterns are currently supported");
		}

	}

}

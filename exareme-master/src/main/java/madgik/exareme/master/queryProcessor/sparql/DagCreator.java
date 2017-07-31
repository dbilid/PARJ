package madgik.exareme.master.queryProcessor.sparql;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.Projection;
import org.eclipse.rdf4j.query.algebra.ProjectionElem;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.QueryParser;
import org.eclipse.rdf4j.query.parser.QueryParserUtil;

import madgik.exareme.master.queryProcessor.decomposer.dag.Node;
import madgik.exareme.master.queryProcessor.decomposer.dag.NodeHashValues;
import madgik.exareme.master.queryProcessor.decomposer.federation.NamesToAliases;
import madgik.exareme.master.queryProcessor.decomposer.query.Column;
import madgik.exareme.master.queryProcessor.decomposer.query.Constant;
import madgik.exareme.master.queryProcessor.decomposer.query.NonUnaryWhereCondition;
import madgik.exareme.master.queryProcessor.decomposer.query.Output;
import madgik.exareme.master.queryProcessor.decomposer.query.Selection;
import madgik.exareme.master.queryProcessor.decomposer.query.Table;

public class DagCreator {

	private ParsedQuery pq;
	private NodeHashValues hashes;
	private int alias;
	private IdFetcher fetcher;

	public DagCreator(ParsedQuery q, int partitions, NodeHashValues hashes, IdFetcher fetcher) {
		super();
		this.pq = q;
		this.hashes = hashes;
		this.fetcher = fetcher;
	}

	public Node getRootNode() throws SQLException {
		Node projection = new Node(Node.AND, Node.PROJECT);
		alias = 1;
		if (pq.getTupleExpr() instanceof Projection) {
			Projection p = (Projection) pq.getTupleExpr();
			Map<JoinClassMap, Node> eqClassesToNodes = new HashMap<JoinClassMap, Node>();

			getNodeFromExpression(eqClassesToNodes, p.getArg());
			List<JoinClassMap> keys = new ArrayList<JoinClassMap>(eqClassesToNodes.keySet());
			JoinClassMap current = keys.remove(0);
			Node top = eqClassesToNodes.get(current);

			while (!keys.isEmpty()) {
				boolean compatibleFound = false;
				for (int i = 0; i < keys.size(); i++) {
					JoinClassMap next = keys.get(i);
					List<NonUnaryWhereCondition> joins = current.merge(next);
					if (!joins.isEmpty()) {
						keys.remove(i);
						Node joinNode = new Node(Node.AND, Node.JOIN);
						joinNode.setObject(joins.get(0));
						joinNode.addChild(top);
						joinNode.addChild(eqClassesToNodes.get(next));
						joinNode.addAllDescendantBaseTables(top.getDescendantBaseTables());
						joinNode.addAllDescendantBaseTables(eqClassesToNodes.get(next).getDescendantBaseTables());
						Node newTop = new Node(Node.OR);
						newTop.addChild(joinNode);
						newTop.addAllDescendantBaseTables(joinNode.getDescendantBaseTables());
						top = newTop;
						compatibleFound = true;
						hashes.put(joinNode.computeHashIDExpand(), joinNode);
						hashes.put(newTop.computeHashIDExpand(), newTop);
						for (int k = 1; k < joins.size(); k++) {
							Node joinNode2 = new Node(Node.AND, Node.JOIN);
							joinNode2.setObject(joins.get(k));
							joinNode2.addChild(top);
							joinNode2.addAllDescendantBaseTables(top.getDescendantBaseTables());
							Node newTop2 = new Node(Node.OR);
							newTop2.addChild(joinNode2);
							newTop2.addAllDescendantBaseTables(joinNode2.getDescendantBaseTables());
							top = newTop2;
							hashes.put(joinNode2.computeHashIDExpand(), joinNode2);
							hashes.put(newTop2.computeHashIDExpand(), newTop2);
						}
						break;
					}
				}
				if (!compatibleFound) {
					throw new SQLException("Input query contains cartesian product. Currently not suppoted");
				}

			}
			projection.addChild(top);
			// Set<Column> projected=new HashSet<Column>();

			madgik.exareme.master.queryProcessor.decomposer.query.Projection prj = new madgik.exareme.master.queryProcessor.decomposer.query.Projection();
			projection.setObject(prj);
			for (ProjectionElem pe : p.getProjectionElemList().getElements()) {
				Column proj = current.getFirstColumn(pe.getSourceName());
				// Column proj= new
				// Column(current.getFirstColumn(pe.getSourceName()).getAlias(),
				// pe.getSourceName());
				// projected.add(proj);
				prj.addOperand(new Output(pe.getTargetName(), proj));
			}
			// System.out.println(projection.dotPrint(new HashSet<Node>()));
			Node root = new Node(Node.OR);
			root.addChild(projection);
			return root;
			// Map<String, Set<Column>> eqClasses=new HashMap<String,
			// Set<Column>>();

		} else {
			throw new SQLException("Input query does not contain projection");
		}

	}

	private JoinClassMap getNodeForTriplePattern(StatementPattern sp, Node top) throws SQLException {
		String predString;
		boolean selection = false;
		String aliasString = null;
		// Node baseTable=new Node(Node.OR);
		Table predTable = null;
		Node selNode = new Node(Node.AND, Node.SELECT);
		Selection s = new Selection();
		selNode.setObject(s);
		JoinClassMap result = new JoinClassMap();
		Var predicate = sp.getPredicateVar();
		Var subject = sp.getSubjectVar();
		Var object = sp.getObjectVar();
		if (!predicate.isConstant()) {
			throw new SQLException("constant predicate not supported yet");
		} else {
			predString = "prop" + fetcher.getIdForProperty(predicate.getValue().stringValue());
			aliasString = "alias" + alias;
			predTable = new Table(predString, aliasString);

			// baseTable.setObject(predTable);
			alias++;
		}
		if (!subject.isConstant()) {
			String varString = subject.getName();
			// if(eqClasses.containsKey(varString)){
			// Set<Column> tablesForVar=eqClasses.get(varString);
			// joinCondition.setLeftOp(tablesForVar.iterator().next());
			Column newCol = new Column(aliasString, "first");
			result.add(varString, newCol);
			// joinCondition.setRightOp(newCol);
			// tablesForVar.add(newCol);
			// }
			// else{
			// Set<Column> tablesForVar=new HashSet<Column>();
			// tablesForVar.add(new Column(aliasString, "s"));
			// eqClasses.put(varString, tablesForVar);
			// }
		} else {
			createSelection(selNode, selection, subject, aliasString, "first");
			selection = true;
		}
		if (!object.isConstant()) {

			String varString = object.getName();

			if (result.containsVar(varString)) {
				throw new SQLException("same var in subject and object not supported yet");
			}

			Column newCol = new Column(aliasString, "second");
			// joinCondition.setRightOp(newCol);
			result.add(varString, newCol);

		} else {
			createSelection(selNode, selection, object, aliasString, "second");
			selection = true;
		}
		if (selection) {
			Node baseNode = new Node(Node.OR);
			baseNode.setObject(predTable);
			hashes.put(baseNode.computeHashIDExpand(), baseNode);
			selNode.addChild(baseNode);
			hashes.put(selNode.computeHashIDExpand(), selNode);
			top.addChild(selNode);
		} else {
			top.setObject(predTable);

		}
		hashes.put(top.computeHashIDExpand(), top);
		top.addDescendantBaseTable(aliasString);
		return result;
	}

	private void createSelection(Node selNode, boolean selection, Var sbjOrObj, String aliasString, String sOrO)
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
		NonUnaryWhereCondition nuwc = new NonUnaryWhereCondition();
		nuwc.setOperator("=");
		nuwc.setLeftOp(new Column(aliasString, sOrO));
		nuwc.setRightOp(new Constant(fetcher.getIdForUri(sbjOrObj.getValue().toString())));
		s.addOperand(nuwc);

	}

	private void getNodeFromExpression(Map<JoinClassMap, Node> eqClassesToNodes, TupleExpr expr) throws SQLException {
		if (expr instanceof Join) {
			Join j = (Join) expr;
			getNodeFromExpression(eqClassesToNodes, j.getLeftArg());
			getNodeFromExpression(eqClassesToNodes, j.getRightArg());
		} else if (expr instanceof StatementPattern) {
			StatementPattern sp = (StatementPattern) expr;
			Node top = new Node(Node.OR);
			eqClassesToNodes.put(getNodeForTriplePattern(sp, top), top);
		} else {
			throw new SQLException("Only basic graph patterns are currently supported");
		}

	}

}
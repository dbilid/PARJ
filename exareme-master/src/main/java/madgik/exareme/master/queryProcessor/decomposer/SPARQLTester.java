package madgik.exareme.master.queryProcessor.decomposer;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
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
import org.eclipse.rdf4j.query.parser.QueryParserUtil;
import org.eclipse.rdf4j.query.parser.sparql.AbstractASTVisitor;
import org.eclipse.rdf4j.query.parser.sparql.ast.ASTProjectionElem;
import org.eclipse.rdf4j.query.parser.sparql.ast.ASTQueryContainer;
import org.eclipse.rdf4j.query.parser.sparql.ast.ASTSelectQuery;
import org.eclipse.rdf4j.query.parser.sparql.ast.ASTVar;
import org.eclipse.rdf4j.query.parser.sparql.ast.ParseException;
import org.eclipse.rdf4j.query.parser.sparql.ast.SyntaxTreeBuilder;
import org.eclipse.rdf4j.query.parser.sparql.ast.TokenMgrError;
import org.eclipse.rdf4j.query.parser.sparql.ast.VisitorException;

import madgik.exareme.master.queryProcessor.decomposer.dag.Node;
import madgik.exareme.master.queryProcessor.decomposer.query.Column;
import madgik.exareme.master.queryProcessor.decomposer.query.Constant;
import madgik.exareme.master.queryProcessor.decomposer.query.NonUnaryWhereCondition;
import madgik.exareme.master.queryProcessor.decomposer.query.Output;
import madgik.exareme.master.queryProcessor.decomposer.query.Selection;
import madgik.exareme.master.queryProcessor.decomposer.query.Table;
import madgik.exareme.master.queryProcessor.sparql.JoinClassMap;

public class SPARQLTester {
	private static int alias;

	public static void main(String[] args) throws TokenMgrError, ParseException, VisitorException, SQLException {
		String prefixes = "PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX ub:<http://swat.cse.lehigh.edu/onto/univ-bench.owl#> ";
		String q = "SELECT ?y ?b ?z  WHERE { ?y ?b ?z . ?a ?v ?z }";
		String q2 = "SELECT ?x ?y ?z WHERE { ?y ub:teacherOf ?z .  ?y rdf:type ub:FullProfessor . ?z rdf:type ub:Course . ?x ub:advisor ?y . ?x rdf:type ub:UndergraduateStudent . ?x ub:takesCourse ?z }";
		long start = System.currentTimeMillis();
		// ParsedQuery pq =
		// QueryParserUtil.parseTupleQuery(QueryLanguage.SPARQL, q,
		// "http://www.ego.com#");
		System.out.println("time " + (System.currentTimeMillis() - start));
		// start=System.currentTimeMillis();
		ASTQueryContainer parseTree0 = SyntaxTreeBuilder.parseQuery(q);
		ParsedQuery pq2 = QueryParserUtil.parseTupleQuery(QueryLanguage.SPARQL, prefixes + q2, "http://www.ego.com#");
		Node projection = new Node(Node.AND, Node.PROJECT);
		alias = 1;
		if (pq2.getTupleExpr() instanceof Projection) {
			Projection p = (Projection) pq2.getTupleExpr();
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
						Node newTop = new Node(Node.OR);
						newTop.addChild(joinNode);
						top = newTop;
						compatibleFound = true;
						break;
					}
				}
				if (!compatibleFound) {
					throw new SQLException("Input query contains cartesian product. Currently not suppoted");
				}

			}
			projection.addChild(top);
			Set<Column> projected = new HashSet<Column>();

			madgik.exareme.master.queryProcessor.decomposer.query.Projection prj = new madgik.exareme.master.queryProcessor.decomposer.query.Projection();
			projection.setObject(prj);
			for (ProjectionElem pe : p.getProjectionElemList().getElements()) {
				Column proj = new Column(current.getFirstColumn(pe.getSourceName()).getAlias(), pe.getSourceName());
				projected.add(proj);
				prj.addOperand(new Output(pe.getTargetName(), proj));
			}
			System.out.println(projection.dotPrint(new HashSet<Node>()));
			// Map<String, Set<Column>> eqClasses=new HashMap<String,
			// Set<Column>>();
			System.out.println("time " + (System.currentTimeMillis() - start));

		}
		// pq2.getTupleExpr().
		System.out.println("time " + (System.currentTimeMillis() - start));
		ASTQueryContainer parseTree = SyntaxTreeBuilder.parseQuery(prefixes + q2);
		System.out.println("time " + (System.currentTimeMillis() - start));
		QueryVariableCollector c = new QueryVariableCollector();
		c.visit(parseTree, null);
		System.out.println("time " + (System.currentTimeMillis() - start));

	}

	private static JoinClassMap getNodeForTriplePattern(StatementPattern sp, Node top) throws SQLException {
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
			predString = predicate.getValue().stringValue();
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
			Column newCol = new Column(aliasString, "s");
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
			createSelection(selNode, selection, subject, aliasString, "s");
			selection = true;
		}
		if (!object.isConstant()) {

			String varString = object.getName();

			if (result.containsVar(varString)) {
				throw new SQLException("same var in subject and object not supported yet");
			}

			Column newCol = new Column(aliasString, "o");
			// joinCondition.setRightOp(newCol);
			result.add(varString, newCol);

		} else {
			createSelection(selNode, selection, object, aliasString, "o");
			selection = true;
		}
		if (selection) {
			Node baseNode = new Node(Node.OR);
			baseNode.setObject(predTable);
			selNode.addChild(baseNode);
			top.addChild(selNode);
		} else {
			top.setObject(predTable);
		}

		return result;
	}

	private static void createSelection(Node selNode, boolean selection, Var sbjOrObj, String aliasString,
			String sOrO) {
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
		nuwc.setRightOp(new Constant(sbjOrObj.getValue().stringValue()));
		s.addOperand(nuwc);

	}

	private static void getNodeFromExpression(Map<JoinClassMap, Node> eqClassesToNodes, TupleExpr expr)
			throws SQLException {
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

class QueryVariableCollector extends AbstractASTVisitor {

	private Set<String> variableNames = new LinkedHashSet<String>();

	public Set<String> getVariableNames() {
		return variableNames;
	}

	@Override
	public Object visit(ASTSelectQuery node, Object data) throws VisitorException {
		// stop visitor from processing body of sub-select, only add variables
		// from the projection
		return visit(node.getSelect(), data);
	}

	@Override
	public Object visit(ASTProjectionElem node, Object data) throws VisitorException {
		// only include the actual alias from a projection element in a
		// subselect, not any variables used as
		// input to a function
		String alias = node.getAlias();
		if (alias != null) {
			variableNames.add(alias);
			return null;
		} else {
			return super.visit(node, data);
		}
	}

	@Override
	public Object visit(ASTVar node, Object data) throws VisitorException {
		if (!node.isAnonymous()) {
			variableNames.add(node.getName());
		}
		return super.visit(node, data);
	}
}

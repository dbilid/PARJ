package madgik.exareme.master.queryProcessor.decomposer.query.visitors;

import java.util.HashMap;
import java.util.Map;

import org.jfree.util.Log;

import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.parser.BinaryRelationalOperatorNode;
import com.foundationdb.sql.parser.CharConstantNode;
import com.foundationdb.sql.parser.ConstantNode;
import com.foundationdb.sql.parser.CursorNode;
import com.foundationdb.sql.parser.FromBaseTable;
import com.foundationdb.sql.parser.FromList;
import com.foundationdb.sql.parser.FromSubquery;
import com.foundationdb.sql.parser.IsNullNode;
import com.foundationdb.sql.parser.JavaToSQLValueNode;
import com.foundationdb.sql.parser.JavaValueNode;
import com.foundationdb.sql.parser.JoinNode;
import com.foundationdb.sql.parser.LikeEscapeOperatorNode;
import com.foundationdb.sql.parser.NotNode;
import com.foundationdb.sql.parser.OrNode;
import com.foundationdb.sql.parser.OrderByList;
import com.foundationdb.sql.parser.ResultSetNode;
import com.foundationdb.sql.parser.SQLToJavaValueNode;
import com.foundationdb.sql.parser.SelectNode;
import com.foundationdb.sql.parser.StaticMethodCallNode;
import com.foundationdb.sql.parser.UnionNode;
import com.foundationdb.sql.parser.Visitable;

import madgik.exareme.master.queryProcessor.decomposer.dag.NodeHashValues;
import madgik.exareme.master.queryProcessor.decomposer.federation.NamesToAliases;
import madgik.exareme.master.queryProcessor.decomposer.query.NonUnaryWhereCondition;
import madgik.exareme.master.queryProcessor.decomposer.query.Operand;
import madgik.exareme.master.queryProcessor.decomposer.query.QueryUtils;
import madgik.exareme.master.queryProcessor.decomposer.query.SQLQuery;
import madgik.exareme.master.queryProcessor.decomposer.query.UDFWhereCondition;
import madgik.exareme.master.queryProcessor.decomposer.query.UnaryWhereCondition;

public class LeftJoinSubVisitor extends AbstractVisitor {

	private boolean stop = false;
	private NodeHashValues hashes;
	private NamesToAliases n2a;

	public LeftJoinSubVisitor(SQLQuery query) {
		super(query);
	}

	public LeftJoinSubVisitor(SQLQuery query, NodeHashValues h, NamesToAliases n2a) {
		super(query);
		hashes = h;
		this.n2a = n2a;
	}

	@Override
	public Visitable visit(Visitable node) throws StandardException {
		if (node instanceof JoinNode) {
			JoinNode jNode = (JoinNode) node;
			if (jNode.getLeftResultSet() instanceof FromBaseTable) {

				FromBaseTableVisitor v = new FromBaseTableVisitor(query);
				jNode.getLeftResultSet().accept(v);

			}
			if (jNode.getRightResultSet() instanceof FromBaseTable) {

				FromBaseTableVisitor v = new FromBaseTableVisitor(query);
				jNode.getRightResultSet().accept(v);

			}

		}

		if (node instanceof CursorNode) {
			CursorNode cNode = (CursorNode) node;
			if (cNode.getFetchFirstClause() != null) {
				query.setLimit((int) (Integer) ((ConstantNode) cNode.getFetchFirstClause()).getValue());
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

		if (node instanceof FromSubquery) {
			FromSubquery from = (FromSubquery) node;
			String alias = from.getCorrelationName();
			ResultSetNode rs = from.getSubquery();

			if (rs instanceof SelectNode) {
				// nested select
				SelectNode nestedSelectNode = (SelectNode) rs;

				SQLQuery nestedSelectSubquery = new SQLQuery();
				// query.readDBInfo();
				SQLQueryVisitor subqueryVisitor = new SQLQueryVisitor(nestedSelectSubquery, hashes, n2a);
				nestedSelectNode.accept(subqueryVisitor);

				this.query.addNestedSelectSubquery(nestedSelectSubquery, alias);
				// ;.nestedSelectSubquery = nestedSelectSubquery;
				// this.query.setNestedSelectSubqueryAlias(alias);
			}
		}
		if (node instanceof NotNode) {
			// IS NOT NULL
			NotNode not = (NotNode) node;
			if (not.getOperand() instanceof IsNullNode) {
				IsNullNode isNull = (IsNullNode) not.getOperand();
				Operand cr = QueryUtils.getOperandFromNode(isNull.getOperand());
				// ColumnReference cr = (ColumnReference) isNull.getOperand();
				UnaryWhereCondition unary = new UnaryWhereCondition(UnaryWhereCondition.IS_NULL, cr, true);
				query.getUnaryWhereConditions().add(unary);
			}
		} else if (node instanceof BinaryRelationalOperatorNode) {
			BinaryRelationalOperatorNode binOp = (BinaryRelationalOperatorNode) node;
			// Do nothing in the inner nodes of the tree
			Operand left = QueryUtils.getOperandFromNode(binOp.getLeftOperand());
			Operand right = QueryUtils.getOperandFromNode(binOp.getRightOperand());
			if (!(left.toString().equals("1") && right.toString().equals("1") && binOp.getOperator().equals("="))) {
				query.getBinaryWhereConditions().add(new NonUnaryWhereCondition(left, right, binOp.getOperator()));
			}
		} else if (node instanceof OrNode) {
			OrNode orOp = (OrNode) node;
			// Do nothing in the inner nodes of the tree
			Operand left = QueryUtils.getOperandFromNode(orOp.getLeftOperand());
			Operand right = QueryUtils.getOperandFromNode(orOp.getRightOperand());
			query.getBinaryWhereConditions().add(new NonUnaryWhereCondition(left, right, orOp.getOperator()));

		} else if (node instanceof LikeEscapeOperatorNode) {
			LikeEscapeOperatorNode like = (LikeEscapeOperatorNode) node;
			Operand c = QueryUtils.getOperandFromNode(like.getReceiver());
			// Column c =
			// new Column(like.getReceiver().getTableName(),
			// like.getReceiver().getColumnName());
			CharConstantNode q = (CharConstantNode) like.getLeftOperand();
			String s = q.getString();
			// System.out.println("dddd");
			UnaryWhereCondition unary = new UnaryWhereCondition(UnaryWhereCondition.LIKE, c, true, s);
			query.getUnaryWhereConditions().add(unary);

		} else if (node instanceof JavaToSQLValueNode) {
			JavaValueNode value = ((JavaToSQLValueNode) node).getJavaValueNode();
			if (value instanceof StaticMethodCallNode) {
				StaticMethodCallNode udf = (StaticMethodCallNode) value;
				UDFWhereCondition udfCondition = new UDFWhereCondition();
				udfCondition.setOperator(udf.getMethodName());
				for (JavaValueNode jvn : udf.getMethodParameters()) {
					if (jvn instanceof SQLToJavaValueNode) {
						SQLToJavaValueNode j = (SQLToJavaValueNode) jvn;
						udfCondition.addOperand(QueryUtils.getOperandFromNode(j.getSQLValueNode()));
					} else {
						Log.error("Uknown UDF parameter type: " + jvn.toString());
					}
				}
				query.addBinaryWhereCondition(udfCondition);
			}
		}

		// if (fl.get(0) instanceof JoinNode) {
		// JoinNode jNode = (JoinNode) fl.get(0);
		// decomposeJoinNode(jNode);
		// }

		// Limit
		// if ().getFetchFirstClause()
		return node;
	}

	@Override
	public boolean skipChildren(Visitable node) throws StandardException {
		return (node instanceof FromSubquery);
	}

}

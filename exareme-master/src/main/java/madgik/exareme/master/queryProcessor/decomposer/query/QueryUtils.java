/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package madgik.exareme.master.queryProcessor.decomposer.query;



import madgik.exareme.master.queryProcessor.decomposer.dag.Node;

import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author dimitris
 */
public class QueryUtils {



	public static Operand convertToMySQLDialect(Operand o) {
		// returns CONCAT('a', b') from 'a' || 'b'
		if (o instanceof BinaryOperand) {
			BinaryOperand bo = (BinaryOperand) o;
			if (bo.getOperator().equalsIgnoreCase("||")) {
				Function result = new Function();
				result.setFunctionName("CONCAT");
				result.addParameter(convertToMySQLDialect(bo.getLeftOp()));
				result.addParameter(convertToMySQLDialect(bo.getRightOp()));
				return result;
			}
			return o;
		}
		if (o instanceof CastOperand) {
			CastOperand co = (CastOperand) o;
			if (co.getCastType().equalsIgnoreCase("INTEGER")) {
				CastOperand signed = new CastOperand(convertToMySQLDialect(co.getCastOp()), "SIGNED");
				return signed;
			}
			if (co.getCastType().equalsIgnoreCase("DOUBLE") || co.getCastType().equalsIgnoreCase("FLOAT")) {
				CastOperand signed = new CastOperand(convertToMySQLDialect(co.getCastOp()), "DECIMAL");
				return signed;
			}
			if (co.getCastType().equalsIgnoreCase("REAL")) {
				CastOperand signed = new CastOperand(convertToMySQLDialect(co.getCastOp()), "DECIMAL");
				return signed;
			}
			if (co.getCastType().equalsIgnoreCase("TIMESTAMP")) {
				CastOperand signed = new CastOperand(convertToMySQLDialect(co.getCastOp()), "DATETIME");
				return signed;
			}
			return co;
		}
		return o;
	}

	static void createOracleVarCharCast(Operand o) {
		if (o instanceof CastOperand) {
			CastOperand co = (CastOperand) o;
			if (co.getCastType().equals("CHAR(8000)")) {
				co.setCastType("VARCHAR(2200)");
			}
		} else if (o instanceof BinaryOperand) {
			BinaryOperand bo = (BinaryOperand) o;
			createOracleVarCharCast(bo.getLeftOp());
			createOracleVarCharCast(bo.getRightOp());
		} else if (o instanceof Function) {
			Function f = (Function) o;
			for (Operand p : f.getParameters()) {
				createOracleVarCharCast(p);
			}
		} else if (o instanceof NonUnaryWhereCondition) {
			NonUnaryWhereCondition nuwc = (NonUnaryWhereCondition) o;
			for (Operand n : nuwc.getOperands()) {
				createOracleVarCharCast(n);
			}
		}

	}


	public static Column getJoinColumnFromOperand(Node op, Operand o, int i) {
		if (o instanceof BinaryOperand) {

			BinaryOperand bo = (BinaryOperand) o;
			if (bo.getOperator().equals("=") && bo.getLeftOp().getAllColumnRefs().size() > 0
					&& bo.getRightOp().getAllColumnRefs().size() > 0) {

				Column col = bo.getLeftOp().getAllColumnRefs().get(0);
				if (op.getChildAt(i).getDescendantBaseTables().contains(col.getAlias())
						&& !op.getChildAt(i).getDescendantBaseTables().contains(col.getAlias())) {
					return col;
				}

				col = bo.getRightOp().getAllColumnRefs().get(0);
				if (op.getChildAt(i).getDescendantBaseTables().contains(col.getAlias())
						&& !op.getChildAt(i).getDescendantBaseTables().contains(col.getAlias())) {
					return col;

				}
			} else if (bo.getOperator().equalsIgnoreCase("and")) {
				Column c2 = getJoinColumnFromOperand(op, bo.getLeftOp(), i);
				if (c2 != null) {
					return c2;
				}
				c2 = getJoinColumnFromOperand(op, bo.getRightOp(), i);
				if (c2 != null) {
					return c2;
				}
			}
		}
		return null;
	}

	public static void reorderBinaryConditions(Operand op, Set<String> left, Set<String> right) {
		if (op instanceof BinaryOperand) {
			BinaryOperand bo = (BinaryOperand) op;
			if (bo.getOperator().equalsIgnoreCase("and")) {
				reorderBinaryConditions(bo.getLeftOp(), left, right);
				reorderBinaryConditions(bo.getRightOp(), left, right);
			} else {
				Operand lo = bo.getLeftOp();
				Operand ro = bo.getRightOp();
				if (lo.getAllColumnRefs().size() > 0 && ro.getAllColumnRefs().size() > 0) {
					if (!left.contains(lo.getAllColumnRefs().get(0).getAlias())) {
						// reorder
						bo.setLeftOp(ro);
						bo.setRightOp(lo);
					}
				}
			}
		}

	}

	public static NonUnaryWhereCondition getJoinCondition(BinaryOperand bo, Node o) {
		NonUnaryWhereCondition result = null;
		if (bo.getLeftOp() instanceof Column && bo.getRightOp() instanceof Column) {
			Column l = (Column) bo.getLeftOp();
			Column r = (Column) bo.getRightOp();
			if (o.getChildAt(0).getDescendantBaseTables().contains(r.getAlias())
					&& o.getChildAt(1).getDescendantBaseTables().contains(l.getAlias())) {
				result = new NonUnaryWhereCondition();
				result.setLeftOp(r);
				result.setRightOp(l);
				result.setOperator(bo.getOperator());
				return result;
			}
			if (o.getChildAt(1).getDescendantBaseTables().contains(r.getAlias())
					&& o.getChildAt(0).getDescendantBaseTables().contains(l.getAlias())) {
				result = new NonUnaryWhereCondition();
				result.setLeftOp(l);
				result.setRightOp(r);
				result.setOperator(bo.getOperator());
				return result;
			}
		}
		if (bo.getLeftOp() instanceof BinaryOperand) {
			result = getJoinCondition((BinaryOperand) bo.getLeftOp(), o);
			if (result != null) {
				return result;
			}
		}
		if (bo.getRightOp() instanceof BinaryOperand) {
			result = getJoinCondition((BinaryOperand) bo.getRightOp(), o);
			return result;
		}
		return null;
	}
}

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package madgik.exareme.master.queryProcessor.decomposer.query;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

import madgik.exareme.master.queryProcessor.decomposer.dag.Node;

/**
 * @author dimitris
 */
public class NonUnaryWhereCondition implements Operand {

	protected List<Operand> ops;
	protected Set<NonUnaryWhereCondition> filterJoins;
	// private Operand leftOp;
	// private Operand rightOp;
	private boolean rightinv;
	private boolean leftinv;
	protected String operator;
	protected HashCode hash = null;

	public NonUnaryWhereCondition() {
		super();
		ops = new ArrayList<Operand>();
	}

	public NonUnaryWhereCondition(Operand left, Operand right, String operator) {
		ops = new ArrayList<Operand>();
		this.ops.add(left);
		this.ops.add(right);
		this.operator = operator;
	}

	public NonUnaryWhereCondition(List<Operand> operands, String operator) {
		this.ops = operands;
		this.operator = operator;
	}

	@Override
	public List<Column> getAllColumnRefs() {
		List<Column> res = new ArrayList<Column>();
		for (Operand o : this.ops) {
			for (Column c : o.getAllColumnRefs()) {
				res.add(c);
			}
		}
		if (this.filterJoins != null) {
			for (NonUnaryWhereCondition f : this.filterJoins) {
				res.addAll(f.getAllColumnRefs());
			}
		}
		return res;
	}

	public void setOperator(String op) {
		this.operator = op;
		hash = null;
	}

	public String getOperator() {
		return this.operator;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		if (operator.equalsIgnoreCase("or") || this.filterJoins != null) {
			sb.append("(");
		}
		sb.append(this.ops.get(0).toString());

		sb.append(" ");
		sb.append(operator);
		sb.append(" ");
		sb.append(ops.get(1).toString());
		for (int i = 2; i < this.ops.size(); i++) {
			// multiway join
			sb.append(" AND ");
			sb.append(ops.get(i - 1).toString());
			sb.append(operator);
			sb.append(" ");
			sb.append(ops.get(i).toString());
		}
		if (filterJoins != null) {
			for (NonUnaryWhereCondition extra : filterJoins) {
				sb.append(" AND ");
				sb.append(extra.toString());
			}
		}
		if (operator.equalsIgnoreCase("or") || this.filterJoins != null) {
			sb.append(")");
		}
		return sb.toString();
	}

	public List<Operand> getOperands() {
		return this.ops;
	}

	public Operand getLeftOp() {
		return this.ops.get(0);
	}

	public Operand getRightOp() {
		return this.ops.get(1);
	}

	public Operand getOp(int i) {
		return this.ops.get(i);
	}

	public void setLeftOp(Operand op) {
		if (this.ops.isEmpty()) {
			this.ops.add(op);
		} else {
			this.ops.set(0, op);
		}
		hash = null;
	}

	public void setRightOp(Operand op) {
		if (this.ops.isEmpty()) {
			this.setLeftOp(new Column(-1, true));
			this.ops.add(op);
		} else if (this.ops.size() == 1) {
			this.ops.add(op);
		} else {
			this.ops.set(1, op);
		}
		hash = null;
	}

	@Override
	public NonUnaryWhereCondition clone() throws CloneNotSupportedException {
		NonUnaryWhereCondition cloned = (NonUnaryWhereCondition) super.clone();
		List<Operand> opsCloned = new ArrayList<Operand>();
		for (Operand o : this.ops) {
			opsCloned.add(o.clone());
		}
		cloned.ops = opsCloned;
		if (filterJoins != null) {
			for (NonUnaryWhereCondition filter : this.filterJoins) {
				cloned.createFilterJoins();
				cloned.addFilterJoin(filter.clone());
			}
		}
		cloned.hash = hash;
		return cloned;
	}

	public void createFilterJoins() {
		this.filterJoins = new HashSet<NonUnaryWhereCondition>();

	}

	public void addFilterJoin(NonUnaryWhereCondition filter) {
		if (filterJoins == null) {
			filterJoins = new HashSet<NonUnaryWhereCondition>();
		}
		filterJoins.add(filter);

	}

	public void setOperandAt(int i, Operand op) {
		this.ops.set(i, op);
		hash = null;
	}

	public void addOperand(Operand op) {
		this.ops.add(op);
		hash = null;
	}

	@Override
	public int hashCode() {
		int hash = 3;
		for (Operand o : this.ops) {
			if (this.operator.equals("=")) {
				// in join commutativity does not affect the result

				hash += (o != null ? o.hashCode() : 0);
			} else {
				// int test=this.ops.hashCode();
				hash = hash * 43 + (o != null ? o.hashCode() : 0);
			}
		}
		hash = hash * 43 + (this.operator != null ? this.operator.hashCode() : 0);
		return hash;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final NonUnaryWhereCondition other = (NonUnaryWhereCondition) obj;
		if (this.ops != other.ops && (this.ops == null || !this.ops.equals(other.ops))) {
			return false;
		}
		if ((this.operator == null) ? (other.operator != null) : !this.operator.equals(other.operator)) {
			return false;
		}
		if (this.filterJoins != other.filterJoins
				&& (this.filterJoins == null || !this.filterJoins.equals(other.filterJoins))) {
			return false;
		}
		return true;
	}

	@Override
	public void changeColumn(Column oldCol, Column newCol) {
		for (int i = 0; i < this.ops.size(); i++) {
			Operand o = this.ops.get(i);
			if (o.getClass().equals(Column.class)) {
				if (((Column) o).equals(oldCol)) {
					this.ops.set(i, newCol);
				}
			} else {
				o.changeColumn(oldCol, newCol);
			}
		}
		if (filterJoins != null) {
			for (NonUnaryWhereCondition f : this.filterJoins) {
				f.changeColumn(oldCol, newCol);
			}
		}
	}



	public void addRangeFilters(NonUnaryWhereCondition bwc) {
		if (bwc.filterJoins != null) {
			this.createFilterJoins();
			for (NonUnaryWhereCondition f : bwc.filterJoins) {
				try {
					this.filterJoins.add(f.clone());
				} catch (CloneNotSupportedException e) {
					e.printStackTrace();
				}
			}
		}

	}

	public Set<NonUnaryWhereCondition> getFilterJoins() {
		return this.filterJoins;
	}

	

	public boolean isRightinv() {
		return rightinv;
	}

	public void setRightinv(boolean rightinv) {
		this.rightinv = rightinv;
	}

	public boolean isLeftinv() {
		return leftinv;
	}

	public void setLeftinv(boolean leftinv) {
		this.leftinv = leftinv;
	}

}

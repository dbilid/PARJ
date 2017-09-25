package madgik.exareme.master.queryProcessor.decomposer.query;

import java.util.ArrayList;
import java.util.List;

public class UDFWhereCondition extends NonUnaryWhereCondition {

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		
		sb.append(this.operator);
		sb.append("(");
		String delimiter = "";
		for (int i = 0; i < this.ops.size(); i++) {
			// multiway join
			sb.append(delimiter);
			sb.append(ops.get(i).toString());
			delimiter = ", ";
		}
		sb.append(")");
		
		return sb.toString();
	}

	@Override
	public UDFWhereCondition clone() throws CloneNotSupportedException {
		UDFWhereCondition cloned = (UDFWhereCondition) super.clone();
		List<Operand> opsCloned = new ArrayList<Operand>();
		for (Operand o : this.ops) {
			opsCloned.add(o.clone());
		}
		cloned.ops = opsCloned;
		
		return cloned;
	}

}

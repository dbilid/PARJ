package madgik.exareme.master.queryProcessor.sparql;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import madgik.exareme.master.queryProcessor.decomposer.query.Column;
import madgik.exareme.master.queryProcessor.decomposer.query.NonUnaryWhereCondition;

public class JoinClassMap {

	private Map<String, JoinClass> eqClasses;

	public JoinClassMap() {
		super();
		eqClasses = new HashMap<String, JoinClass>();
	}

	public List<NonUnaryWhereCondition> merge(JoinClassMap other) {
		List<NonUnaryWhereCondition> joinConditions = new ArrayList<NonUnaryWhereCondition>();
		Set<String> notContained = new HashSet<String>();
		for (String otherVar : other.eqClasses.keySet()) {
			if (this.eqClasses.containsKey(otherVar)) {
				NonUnaryWhereCondition joinCondition = new NonUnaryWhereCondition();
				joinCondition.setOperator("=");
				joinCondition.setLeftOp(eqClasses.get(otherVar).getFirstColumn());
				joinCondition.setRightOp(other.eqClasses.get(otherVar).getFirstColumn());
				joinConditions.add(joinCondition);

				this.eqClasses.get(otherVar).merge(other.eqClasses.get(otherVar));
			} else {
				notContained.add(otherVar);
			}
		}
		if (!joinConditions.isEmpty()) {
			for (String n : notContained) {
				this.eqClasses.put(n, other.eqClasses.get(n));
			}

		}
		return joinConditions;
	}

	public void add(String varString, Column newCol) {
		if(eqClasses.containsKey(varString)){
			eqClasses.get(varString).addColumn(newCol);
		}else{
			eqClasses.put(varString, new JoinClass(newCol));
		}
	}

	public boolean containsVar(String varString) {
		return eqClasses.containsKey(varString);
	}

	public Column getFirstColumn(String var) {
		return this.eqClasses.get(var).getFirstColumn();
	}

	public Collection<JoinClass> getClasses() {
		return eqClasses.values();
	}

}

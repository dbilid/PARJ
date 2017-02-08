package madgik.exareme.master.queryProcessor.sparql;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import madgik.exareme.master.queryProcessor.decomposer.query.Column;
import madgik.exareme.master.queryProcessor.decomposer.query.NonUnaryWhereCondition;

public class JoinClassMap {
	
	private Map<String, JoinClass> eqClasses;

	public JoinClassMap() {
		super();
		eqClasses=new HashMap<String, JoinClass>();
	}
	
	public NonUnaryWhereCondition merge(JoinClassMap other){
		NonUnaryWhereCondition joinCondition=null;
		Set<String> notContained=new HashSet<String>();
		for(String otherVar:other.eqClasses.keySet()){
			if(this.eqClasses.containsKey(otherVar)){
				if(joinCondition==null){
					joinCondition=new NonUnaryWhereCondition();
					joinCondition.setOperator("=");
					joinCondition.setLeftOp(eqClasses.get(otherVar).getFirstColumn());
					joinCondition.setRightOp(other.eqClasses.get(otherVar).getFirstColumn());
				}
				else{
					 NonUnaryWhereCondition filterJoin=new NonUnaryWhereCondition();
					 filterJoin.setOperator("=");
					 filterJoin.setLeftOp(eqClasses.get(otherVar).getFirstColumn());
					 filterJoin.setRightOp(other.eqClasses.get(otherVar).getFirstColumn());
					 joinCondition.addFilterJoin(filterJoin);
				}
				this.eqClasses.get(otherVar).merge(other.eqClasses.get(otherVar));
			}
			else{
				notContained.add(otherVar);
			}
		}
		if(joinCondition!=null){
			for(String n:notContained){
				this.eqClasses.put(n, other.eqClasses.get(n));
			}
			
		}
		return joinCondition;
	}

	public void add(String varString, Column newCol) {
		eqClasses.put(varString, new JoinClass(newCol));
		
	}

	public boolean containsVar(String varString) {
		return eqClasses.containsKey(varString);
	}
	
	public Column getFirstColumn(String var){
		return this.eqClasses.get(var).getFirstColumn();
	}

}

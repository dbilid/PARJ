package madgik.exareme.master.queryProcessor.decomposer;

import java.util.HashSet;
import java.util.Set;

import madgik.exareme.master.queryProcessor.decomposer.query.NonUnaryWhereCondition;
import madgik.exareme.master.queryProcessor.decomposer.query.UnaryWhereCondition;

public class ViewInfo {

	private Set<String> outColumns;
	private Set<UnaryWhereCondition> unaryConditions;
	private Set<NonUnaryWhereCondition> binaryConditions;
	private String viewName;
	private boolean or;

	public ViewInfo(String name, Set<String> outputs) {
		super();
		this.viewName = name;
		this.outColumns = outputs;
		unaryConditions = new HashSet<UnaryWhereCondition>();
		binaryConditions = new HashSet<NonUnaryWhereCondition>();
		or=false;
	}

	public boolean addCondition(Object toAdd) {
		if (toAdd instanceof NonUnaryWhereCondition)
			return binaryConditions.add((NonUnaryWhereCondition) toAdd);
		else if (toAdd instanceof UnaryWhereCondition)
			return unaryConditions.add((UnaryWhereCondition) toAdd);
		else
			return false;
	}


	

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((binaryConditions == null) ? 0 : binaryConditions.hashCode());
		result = prime * result + ((outColumns == null) ? 0 : outColumns.hashCode());
		result = prime * result + ((unaryConditions == null) ? 0 : unaryConditions.hashCode());
		result = prime * result + ((viewName == null) ? 0 : viewName.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ViewInfo other = (ViewInfo) obj;
		if (binaryConditions == null) {
			if (other.binaryConditions != null)
				return false;
		} else if (!binaryConditions.equals(other.binaryConditions))
			return false;
		if (outColumns == null) {
			if (other.outColumns != null)
				return false;
		} else if (!outColumns.equals(other.outColumns))
			return false;
		if (unaryConditions == null) {
			if (other.unaryConditions != null)
				return false;
		} else if (!unaryConditions.equals(other.unaryConditions))
			return false;
		if (viewName == null) {
			if (other.viewName != null)
				return false;
		} else if (!viewName.equals(other.viewName))
			return false;
		return true;
	}

	public boolean containsCondition(Object o) {
		if (o instanceof NonUnaryWhereCondition){
			if(binaryConditions.size()==1){
			return binaryConditions.iterator().next().equals(o);}
			else{
				return binaryConditions.contains(o);
			}
		}
		else if (o instanceof UnaryWhereCondition)
			return unaryConditions.contains(o);
		else
			return false;
	}

	public Set<String> getOutput() {
		return this.outColumns;
	}

	public String getTableName() {
		return this.viewName;
	}

	public int getNumberOfConditions() {
		if(this.or){
			return 1;
		}
		else{
			return unaryConditions.size()+binaryConditions.size();
		}
	}

	public void addConditions(ViewInfo other) {
		this.binaryConditions.addAll(other.binaryConditions);
		this.unaryConditions.addAll(other.unaryConditions);
		
	}

	public boolean isOr() {
		return or;
	}

	public void setOr(boolean or) {
		this.or = or;
	}

	public boolean orsAreEqual(Set<NonUnaryWhereCondition> ors) {
		return ors.equals(this.binaryConditions);
	}
	
	

}

package madgik.exareme.master.queryProcessor.decomposer.dp;

import java.util.HashSet;
import java.util.Set;

import madgik.exareme.master.queryProcessor.decomposer.query.Column;
import madgik.exareme.master.queryProcessor.decomposer.query.NonUnaryWhereCondition;
import madgik.exareme.master.queryProcessor.decomposer.query.Operand;
import madgik.exareme.master.queryProcessor.estimator.NodeInfo;



public class DPEntry {
	
	NodeInfo stats;
	double cost;
	int order[];
	private Set<Column> sorted;
	
	
	public DPEntry(NodeInfo stats, double cost2, int initialSize) {
		super();
		this.stats = stats;
		this.cost = cost2;
		this.order=new int[initialSize];
	}
	
	public DPEntry(NodeInfo resultInfo, double cost2, DPEntry p1, DPEntry p2) {
		super();
		this.stats = resultInfo;
		this.cost = cost2 + p1.cost +p2.cost;
		order=new int[p1.noOfInputTables()+1];
		System.arraycopy(p1.order, 0, order, 0, p1.noOfInputTables());
		   System.arraycopy(p2.order, 0, order, p1.noOfInputTables(), 1);
	}

	public double getCost() {
		return cost;
	}

	public void addOrder(int i) {
		order[0]=i;
		
	}

	public int noOfInputTables(){
		return order.length;
	}

	public boolean isSortedOn(Operand c) {
		if(sorted==null){
			return false;
		}
		if(c instanceof Column){
			return sorted.contains((Column)c);
		}
		return false;
	}

	public void setSorted(NonUnaryWhereCondition join) {
		if(sorted==null){
			sorted=new HashSet<Column>();
		}
		for (Column c:join.getAllColumnRefs()){
			sorted.add(c);
		}
	}

	

	public void addSorted(DPEntry p1) {
		for(Column c:p1.sorted){
			sorted.add(c);
		}
		
	}
	
	
	
}

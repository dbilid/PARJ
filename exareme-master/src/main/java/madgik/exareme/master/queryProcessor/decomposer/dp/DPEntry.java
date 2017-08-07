package madgik.exareme.master.queryProcessor.decomposer.dp;

import madgik.exareme.master.queryProcessor.estimator.NodeInfo;



public class DPEntry {
	
	NodeInfo stats;
	double cost;
	int order[];
	
	
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
	
}

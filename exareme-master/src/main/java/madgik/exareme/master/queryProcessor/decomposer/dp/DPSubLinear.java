package madgik.exareme.master.queryProcessor.decomposer.dp;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import madgik.exareme.master.queryProcessor.decomposer.dag.Node;
import madgik.exareme.master.queryProcessor.decomposer.query.NonUnaryWhereCondition;
import madgik.exareme.master.queryProcessor.decomposer.query.Table;
import madgik.exareme.master.queryProcessor.estimator.NodeCostEstimator;
import madgik.exareme.master.queryProcessor.estimator.NodeInfo;
import madgik.exareme.master.queryProcessor.estimator.NodeSelectivityEstimator;

public class DPSubLinear {

	private List<Node> tables;
	private DPEntry[] entries;
	private NodeSelectivityEstimator nse;
	private Map<Integer, Set<Integer>> connectedTables;
	private EquivalentColumnClasses classes;

	public DPSubLinear(List<Node> tables, EquivalentColumnClasses classes) {
		super();
		this.classes = classes;
		this.tables = tables;
		this.entries = new DPEntry[(int) Math.pow(2, tables.size())];
		computeConnectedTables();
	}

	private void computeConnectedTables() {
		connectedTables = new HashMap<Integer, Set<Integer>>();
		for (int i = 1; i < tables.size() + 1; i++) {
			connectedTables.put(i, new HashSet<Integer>());
		}
		classes.computeConnectedTables(connectedTables);
	}

	public int[] getPlan() {
		// for(int i=1;i<tables.size()+1;i++){
		// DPEntry baseTable=new DPEntry(tables.get(i).getNodeInfo(), 0);
		// entries[i]=baseTable;
		// }
		// System.out.println("connectedTables:"+this.connectedTables);
		for (int i = 1; i < entries.length; i++) {
			// System.out.println("i:"+i);
			Set<Integer> nodesInEntry = getNodesForEntry(i);
			double minCost=Double.MAX_VALUE;
			// System.out.println("nodesInEntry:"+nodesInEntry);
			for (int rj : nodesInEntry) {
				
				
				
				int rest = i ^ (1 << (rj - 1));
				// System.out.println("rest:"+rest);
				if (rest == 0) {
					DPEntry baseTable = new DPEntry(tables.get(rj - 1)
							.getNodeInfo(), 0, 1);
					baseTable.addOrder(rj - 1);
					entries[i] = baseTable;
					continue;
				}
				DPEntry p1 = entries[rest];
				if (p1 == null) {
					continue;
				}
				if(p1.getCost()>minCost){
					continue;
				}
				classes.renew();
				Set<NonUnaryWhereCondition> joins = new HashSet<NonUnaryWhereCondition>();
				for (Integer connected : this.connectedTables.get(rj)) {
					if ((1 << (connected - 1) & rest) > 0) {
						Set<NonUnaryWhereCondition> join = classes.getJoin(
								connected, rj);
						if (join != null) {
							joins.addAll(join);
						}
					}
				}
				if (joins.isEmpty()) {
					continue;
				}
				NodeInfo resultInfo = null;
				if (entries[i] != null) {
					resultInfo = entries[i].stats;
				}
				DPEntry p2 = entries[1 << (rj - 1)];
				DPEntry newEntry = createJoinTree(p1, p2, joins, resultInfo);
				if (newEntry != null) {
					
					if (minCost > newEntry.getCost()) {
						//System.out.print("i:"+i+" order:");
						//for(int o=0;o<newEntry.order.length;o++){
						//	System.out.print(newEntry.order[o]);
						//}
						//System.out.println(" cost:"+newEntry.cost);
						//System.out.println("tuples:"+newEntry.stats.getNumberOfTuples());
						entries[i] = newEntry;
						minCost=newEntry.getCost();
					}
				}
			}
		}
		System.out.println("join order done.");
		//System.out.print("orer: ");
		for (int i = 0; i < entries[entries.length - 1].order.length; i++) {
			System.out.print(entries[entries.length - 1].order[i]);
		}
		System.out.println("");
		return entries[entries.length - 1].order;
	}

	private DPEntry createJoinTree(DPEntry p1, DPEntry p2, Set<NonUnaryWhereCondition> joins, NodeInfo resultInfo) {
		boolean filterjoin=false;
		//if(resultInfo==null){
		for(NonUnaryWhereCondition join:joins){
			if(!filterjoin){
		
			resultInfo=nse.estimateJoin(p1.stats, p2.stats, join);
		filterjoin=true;
		}
			
			else{
				resultInfo=nse.estimateFilterJoin(resultInfo, join);
			}
		}
			//}
	
		for(NonUnaryWhereCondition join:joins){
			//filter joins???
			double cost;
			if(p1.order.length==1){
				cost=NodeCostEstimator.getCostForScan(p1.stats, p2.stats, join);
				DPEntry result=new DPEntry(resultInfo, cost, p1, p2);
				result.setSorted(join);
				return result;
			}
			else if(p1.isSortedOn(join.getLeftOp())){
				cost=NodeCostEstimator.getCostForScan(p1.stats, p2.stats, join);
				DPEntry result=new DPEntry(resultInfo, cost, p1, p2);
				result.setSorted(join);
				result.addSorted(p1);
				return result;
			}
			else{
				cost=NodeCostEstimator.getCostForBinarySearch(p1.stats, p2.stats, join);
				DPEntry result=new DPEntry(resultInfo, cost, p1, p2);
				return result;
			}
		
		}
			
		
		return null;
	}

	private Set<Integer> getNodesForEntry(int n) {
		Set<Integer> result = new HashSet<Integer>();
		for (int i = 0; i < tables.size(); i++) {
			if ((n >> i) % 2 == 1) {
				result.add(i + 1);
			}
		}
		return result;
	}

	public void setNse(NodeSelectivityEstimator nse2) {
		this.nse = nse2;

	}

}

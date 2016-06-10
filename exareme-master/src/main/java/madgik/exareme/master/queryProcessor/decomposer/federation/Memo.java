/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package madgik.exareme.master.queryProcessor.decomposer.federation;

import madgik.exareme.master.queryProcessor.decomposer.dag.Node;
import madgik.exareme.master.queryProcessor.decomposer.dag.PartitionCols;
import madgik.exareme.master.queryProcessor.decomposer.query.Column;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author dimitris
 */
public class Memo {
    private final Map<MemoKey, MemoValue> memo = new HashMap<MemoKey, MemoValue>();

    public Memo() {
    }

    public MemoValue getMemoValue(MemoKey k) {
        return memo.get(k);
    }

    public boolean containsMemoKey(MemoKey ec) {
        return memo.containsKey(ec);
    }

    public void put(Node e, SinglePlan resultPlan, Column c, double repCost, PartitionCols l, Set<MemoKey> toMaterialize) {
        MemoKey k = new MemoKey(e, c);
        PartitionedMemoValue v = new PartitionedMemoValue(resultPlan, repCost);
        v.setDlvdPart(l);
        v.setToMat(toMaterialize);
        memo.put(k, v);

    }

    public void put(Node e, SinglePlan resultPlan, Column c, double repCost, boolean b,
        PartitionCols l) {
        MemoKey k = new MemoKey(e, c);
        PartitionedMemoValue v = new PartitionedMemoValue(resultPlan, repCost);
        v.setMaterialized(b);
        v.setDlvdPart(l);
        memo.put(k, v);
    }

    public void setPlanUsed(MemoKey e) {
        MemoValue v =  getMemoValue(e);
        if(v.isMaterialised()){
       // 	v.setUsed(true);
        	return;
        }
       // else 
    //    if(v.getUsed()){
        	//v.setMaterialized(true);
       // }
        v.addUsed(1);
        
        SinglePlan p = v.getPlan();
        for (int i = 0; i < p.noOfInputPlans(); i++) {
            MemoKey sp = p.getInputPlan(i);
            
            if(sp.getNode().getDescendantBaseTables().size()==1 && !this.getMemoValue(sp).isFederated()){
            	continue;            	
            }
            setPlanUsed(sp);

        }

    }

    public void put(Node e, SinglePlan resultPlan, boolean used, boolean federated) {
        MemoKey k = new MemoKey(e, null);
        CentralizedMemoValue v = new CentralizedMemoValue(resultPlan);
        v.setFederated(federated);
        memo.put(k, v);

    }

    public void put(Node e, SinglePlan resultPlan, boolean materialized, boolean used,
        boolean federated) {
        MemoKey k = new MemoKey(e, null);
        CentralizedMemoValue v = new CentralizedMemoValue(resultPlan);
        v.setFederated(federated);
        v.setMaterialized(materialized);
        memo.put(k, v);

    }

	public void removeUsageFromChildren(MemoKey ec, int times, int unionNo) {
		CentralizedMemoValue v = (CentralizedMemoValue) getMemoValue(ec);
	        if(v.isMaterialised()&& v.getMatUnion()!=unionNo&&v.getUsed()-times>1){

	       // 	v.setUsed(true);
	        	return;
	        }
	       // else 
	    //    if(v.getUsed()){
	        	//v.setMaterialized(true);
	       // }
	        v.addUsed(-times);
	        v.setMaterialized(false);
	        
	        SinglePlan p = v.getPlan();
	        for (int i = 0; i < p.noOfInputPlans(); i++) {
	            MemoKey sp = p.getInputPlan(i);
	            
	            if(sp.getNode().getDescendantBaseTables().size()==1 && !this.getMemoValue(sp).isFederated()){
	            	continue;            	
	            }
	            removeUsageFromChildren(sp, times, unionNo);

	        }
		
	}



}

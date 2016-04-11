package madgik.exareme.master.queryProcessor.decomposer.federation;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import madgik.exareme.master.queryProcessor.decomposer.dag.Node;

public class SipToUnions {
	
	private Map<Integer, Set<SipNode>> sipToUnions;

	public SipToUnions() {
		super();
		this.sipToUnions=new HashMap<Integer, Set<SipNode>>();
	}
	
	public void put(int i, Set<SipNode> sips){
		sipToUnions.put(i, sips);
	}
	
	public Set<SipNode> get(int i){
		return sipToUnions.get(i);
	}

	public SipInfo getSipInfo(int unionNo, Node node) {
		Set<SipNode> unionSip=sipToUnions.get(unionNo);
		if(unionSip!=null){
			for(SipNode sn:unionSip){
				if(sn.getNode().equals(node.getChildAt(0))){
					return sn.getSipInfo();
				}
				if(sn.getSipInfo().getJoinNode().equals(node.getChildAt(1).getObject().toString())){
					return sn.getSipInfo();
				}
			}
		}
		return null;
	}

}

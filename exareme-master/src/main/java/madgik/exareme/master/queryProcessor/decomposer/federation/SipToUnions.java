package madgik.exareme.master.queryProcessor.decomposer.federation;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import madgik.exareme.master.queryProcessor.decomposer.dag.Node;

public class SipToUnions {

	private Map<String, Set<SipNode>> sipToUnions;

	public SipToUnions() {
		super();
		this.sipToUnions = new HashMap<String, Set<SipNode>>();
	}

	public void put(String i, Set<SipNode> sips) {
		sipToUnions.put(i, sips);
	}

	public Set<SipNode> get(String i) {
		return sipToUnions.get(i);
	}

	public Set<SipNode> getSipInfo(String unionNo, Node node) {
		Set<SipNode> result=new HashSet<SipNode>();
		Set<SipNode> unionSip = sipToUnions.get(unionNo);
		if (unionSip != null) {
			for (SipNode sn : unionSip) {
				if (sn.getNode().equals(node.getChildAt(0))) {
					result.add(sn);
					//break;
				}
				if (sn.getSipInfo().getJoinNode().equals(node.getChildAt(1).getObject().toString())) {
					result.add(sn);
					//break;
				}
			}
		}
		return result;
	}
	
	public Set<SipNode> getSipInfo(String unionNo, Node base, boolean isbase) {
		Set<SipNode> result=new HashSet<SipNode>();
		Set<SipNode> unionSip = sipToUnions.get(unionNo);
		if (unionSip != null) {
			for (SipNode sn : unionSip) {
				if (sn.getNode().equals(base)) {
					result.add(sn);
					//break;
				}
				
			}
		}
		return result;
	}

}

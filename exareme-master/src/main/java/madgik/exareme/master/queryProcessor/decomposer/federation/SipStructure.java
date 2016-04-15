package madgik.exareme.master.queryProcessor.decomposer.federation;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import madgik.exareme.master.queryProcessor.decomposer.dag.Node;
import madgik.exareme.master.queryProcessor.decomposer.query.Column;
import madgik.exareme.master.queryProcessor.decomposer.query.NonUnaryWhereCondition;
import madgik.exareme.master.queryProcessor.decomposer.query.Projection;
import madgik.exareme.master.queryProcessor.decomposer.query.Table;

public class SipStructure {
	private final Map<SipInfo, Set<SipInfoValue>> sipInfos = new HashMap<SipInfo, Set<SipInfoValue>>();

	public void addToSipInfo(Projection p, Node n, Set<SipNode> set) {
		NonUnaryWhereCondition join = (NonUnaryWhereCondition) n.getObject();
		Node left = n.getChildAt(0);
		Node right = n.getChildAt(1);
		if (!(join.getLeftOp() instanceof Column && join.getRightOp() instanceof Column)) {
			return;
		}
		SipInfo si = new SipInfo(p, join.getLeftOp().getAllColumnRefs().get(0), left);
		boolean exists = false;
		SipInfoValue siv=new SipInfoValue(right, si.AnonymizeColumns());
		for (SipInfo siKey : sipInfos.keySet()) {
			if (siKey.equals(si)) {
				Set<SipInfoValue> nodes = sipInfos.get(siKey);
				nodes.add(siv);
				set.add(new SipNode(right, siKey));
				exists = true;
				break;
			}
		}
		if (!exists) {
			Set<SipInfoValue> s = new HashSet<SipInfoValue>();
			s.add(siv);
			sipInfos.put(si, s);
			set.add(new SipNode(right, si));
		}
		exists = false;
		si = new SipInfo(p, join.getRightOp().getAllColumnRefs().get(0), right);
		siv=new SipInfoValue(left, si.AnonymizeColumns());
		for (SipInfo siKey : sipInfos.keySet()) {
			if (siKey.equals(si)) {
				Set<SipInfoValue> nodes = sipInfos.get(siKey);
				nodes.add(siv);
				set.add(new SipNode(left, siKey));
				exists = true;
				break;
			}
		}
		if (!exists) {
			Set<SipInfoValue> s = new HashSet<SipInfoValue>();
			s.add(siv);
			sipInfos.put(si, s);
			set.add(new SipNode(left, si));
		}

	}

	public void removeNotNeededSIPs() {
		System.out.println(sipInfos);
		Set<SipInfo> toRemove = new HashSet<SipInfo>();
		for (SipInfo si : this.sipInfos.keySet()) {
			if (sipInfos.get(si).size() == 1) {
				toRemove.add(si);
			}
		}
		for (SipInfo si : toRemove) {
			sipInfos.remove(si);
		}
		System.out.println(sipInfos);
	}

	public void markSipUsed(Projection p, Node n) {
		NonUnaryWhereCondition join = (NonUnaryWhereCondition) n.getObject();
		Node left = n.getChildAt(0);
		Node right = n.getChildAt(1);
		if (!(join.getLeftOp() instanceof Column && join.getRightOp() instanceof Column)) {
			return;
		}
		SipInfo si = new SipInfo(p, join.getLeftOp().getAllColumnRefs().get(0), left);
		for (SipInfo key : sipInfos.keySet()) {
			if (si.equals(key)) {
				key.increaseCounter();
				break;
			}
		}
		si = new SipInfo(p, join.getRightOp().getAllColumnRefs().get(0), right);
		for (SipInfo key : sipInfos.keySet()) {
			if (si.equals(key)) {
				key.increaseCounter();
				break;
			}
		}

	}

	public void resetCounters() {
		for (SipInfo si : this.sipInfos.keySet()) {
			si.resetCounter();
		}
	}

	public Table getSipName(Node op, Projection projection) {
		for (SipInfo si : this.sipInfos.keySet()) {
			if (si.getCounter() > 1 && si.getJoinNode().equals(op.getChildAt(0).getObject().toString())
					|| si.getJoinNode().equals(op.getChildAt(1).getObject().toString())) {
				return new Table(si.getName(), si.getName());
			}
		}
		return new Table("no", "no");
	}

	public SipInfo getSipInfo(Node eq, Projection object) {
		for (SipInfo si : this.sipInfos.keySet()) {
			if (si.getJoinNode().equals(eq.getObject().toString())) {
				return si;
			}
		}
		return null;
	}

	public void printMultiUsed() {
		for(SipInfo si:sipInfos.keySet()){
			if(si.getCounter()>1){
				System.out.println(si);
			}
		}
		
	}

	public Set<SipInfoValue> getSipInfo(SipInfo si) {
		return sipInfos.get(si);
	}

}

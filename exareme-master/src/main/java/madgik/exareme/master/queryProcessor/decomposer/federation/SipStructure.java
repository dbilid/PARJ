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
		add(left, right, p, set, join);
		if (!left.getChildren().isEmpty()) {
			// if we have filter join also add the child node
			for (Node n2 : left.getChildren()) {
				if (n2.getChildren().size() == 1 && n2.getObject() instanceof NonUnaryWhereCondition) {
					add(n2.getChildAt(0), right, p, set, join);
				}
			}
		}
		if (!left.getChildren().isEmpty()) {
			// if we have filter join add also the child node
			for (Node n2 : right.getChildren()) {
				if (n2.getChildren().size() == 1 && n2.getObject() instanceof NonUnaryWhereCondition) {
					add(left, n2.getChildAt(0), p, set, join);
				}
			}
		}

	}

	private void add(Node left, Node right, Projection p, Set<SipNode> set, NonUnaryWhereCondition join) {
		SipInfo si = new SipInfo(p, join.getLeftOp().getAllColumnRefs().get(0), left);
		boolean exists = false;
		SipInfoValue siv = new SipInfoValue(right, si.AnonymizeColumns());
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
		siv = new SipInfoValue(left, si.AnonymizeColumns());
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
		// System.out.println(sipInfos);
		System.out.println("original sip size:" + sipInfos.size());
		Set<SipInfo> toRemove = new HashSet<SipInfo>();
		for (SipInfo si : this.sipInfos.keySet()) {
			if (sipInfos.get(si).size() == 1) {
				toRemove.add(si);
			}
		}
		for (SipInfo si : toRemove) {
			sipInfos.remove(si);
		}
		System.out.println("sip size:" + sipInfos.size());
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
			if (si.getCounter() > 0 && si.getJoinNode().equals(op.getChildAt(0).getObject().toString())
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
		for (SipInfo si : sipInfos.keySet()) {
			if (si.getCounter() > 1) {
				System.out.println(si);
			}
		}

	}

	public Set<SipInfoValue> getSipInfo(SipInfo si) {
		return sipInfos.get(si);
	}

	public void addToSipInfo(Projection p, Node n, Set<SipNode> set, NonUnaryWhereCondition nuwc) {
		NonUnaryWhereCondition join = (NonUnaryWhereCondition) n.getObject();
		Node left = n.getChildAt(0);
		Node right = n.getChildAt(1);
		if (!(join.getLeftOp() instanceof Column && join.getRightOp() instanceof Column)) {
			return;
		}
		if (!(nuwc.getLeftOp() instanceof Column && nuwc.getRightOp() instanceof Column)) {
			return;
		}
		Column rightFilterColumn = null;
		Column leftFilterColumn = null;
		if (!join.equals(nuwc)) {
			if (right.isDescendantOfBaseTable(nuwc.getLeftOp().getAllColumnRefs().get(0).getAlias())) {
				rightFilterColumn = nuwc.getLeftOp().getAllColumnRefs().get(0);
			}
			if (left.isDescendantOfBaseTable(nuwc.getLeftOp().getAllColumnRefs().get(0).getAlias())) {
				leftFilterColumn = nuwc.getLeftOp().getAllColumnRefs().get(0);
			}
			if (right.isDescendantOfBaseTable(nuwc.getRightOp().getAllColumnRefs().get(0).getAlias())) {
				rightFilterColumn = nuwc.getRightOp().getAllColumnRefs().get(0);
			}
			if (left.isDescendantOfBaseTable(nuwc.getRightOp().getAllColumnRefs().get(0).getAlias())) {
				leftFilterColumn = nuwc.getRightOp().getAllColumnRefs().get(0);
			}
		}
		// if(leftFilterColumn==null || rightFilterColumn==null){
		// return;
		// }
		SipInfo si = new SipInfo(p, join.getLeftOp().getAllColumnRefs().get(0), left);
		if (leftFilterColumn != null && rightFilterColumn != null) {
			si.addJoinCol(rightFilterColumn);
		}
		boolean exists = false;
		SipInfoValue siv = new SipInfoValue(right, si.AnonymizeColumns());
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
		if (leftFilterColumn != null && rightFilterColumn != null) {
			si.addJoinCol(leftFilterColumn);
		}
		siv = new SipInfoValue(left, si.AnonymizeColumns());
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

	public void addToSipInfo(Projection p, CartesianSip cs, Set<SipNode> set) {
		SipInfo si = new SipInfo(p, cs.getC(), cs.getCommon());
		boolean exists = false;
		SipInfoValue siv = new SipInfoValue(cs.getOther(), si.AnonymizeColumns());
		for (SipInfo siKey : sipInfos.keySet()) {
			if (siKey.equals(si)) {
				Set<SipInfoValue> nodes = sipInfos.get(siKey);
				nodes.add(siv);
				set.add(new SipNode(cs.getOther(), siKey));
				exists = true;
				break;
			}
		}
		if (!exists) {
			Set<SipInfoValue> s = new HashSet<SipInfoValue>();
			s.add(siv);
			sipInfos.put(si, s);
			set.add(new SipNode(cs.getOther(), si));
		}

	}

	public void addToSipInfo(Projection p, Set<SipNode> set, Node other) {
		//query without any join
		Node empty=new Node(Node.OR);
		empty.setObject("empty");
		SipInfo si = new SipInfo(p, null, empty);
		boolean exists = false;
		SipInfoValue siv = new SipInfoValue(other, si.AnonymizeColumns());
		for (SipInfo siKey : sipInfos.keySet()) {
			if (siKey.equals(si)) {
				Set<SipInfoValue> nodes = sipInfos.get(siKey);
				nodes.add(siv);
				set.add(new SipNode(other, siKey));
				exists = true;
				break;
			}
		}
		if (!exists) {
			Set<SipInfoValue> s = new HashSet<SipInfoValue>();
			s.add(siv);
			sipInfos.put(si, s);
			set.add(new SipNode(other, si));
		}
		
	}

	public void addToSipInfo(Projection p, Node n, Set<SipNode> set, NonUnaryWhereCondition nuwc,
			NonUnaryWhereCondition nuwc2) {
		NonUnaryWhereCondition join = (NonUnaryWhereCondition) n.getObject();
		Node left = n.getChildAt(0);
		Node right = n.getChildAt(1);
		if (!(join.getLeftOp() instanceof Column && join.getRightOp() instanceof Column)) {
			return;
		}
		if (!(nuwc.getLeftOp() instanceof Column && nuwc.getRightOp() instanceof Column)) {
			return;
		}
		if (!(nuwc2.getLeftOp() instanceof Column && nuwc2.getRightOp() instanceof Column)) {
			return;
		}
		Column rightFilterColumn = null;
		Column leftFilterColumn = null;
		if (!join.equals(nuwc)) {
			if (right.isDescendantOfBaseTable(nuwc.getLeftOp().getAllColumnRefs().get(0).getAlias())) {
				rightFilterColumn = nuwc.getLeftOp().getAllColumnRefs().get(0);
			}
			if (left.isDescendantOfBaseTable(nuwc.getLeftOp().getAllColumnRefs().get(0).getAlias())) {
				leftFilterColumn = nuwc.getLeftOp().getAllColumnRefs().get(0);
			}
			if (right.isDescendantOfBaseTable(nuwc.getRightOp().getAllColumnRefs().get(0).getAlias())) {
				rightFilterColumn = nuwc.getRightOp().getAllColumnRefs().get(0);
			}
			if (left.isDescendantOfBaseTable(nuwc.getRightOp().getAllColumnRefs().get(0).getAlias())) {
				leftFilterColumn = nuwc.getRightOp().getAllColumnRefs().get(0);
			}
		}
		
		Column rightFilterColumn2 = null;
		Column leftFilterColumn2 = null;
		if (!join.equals(nuwc2)) {
			if (right.isDescendantOfBaseTable(nuwc2.getLeftOp().getAllColumnRefs().get(0).getAlias())) {
				rightFilterColumn2 = nuwc2.getLeftOp().getAllColumnRefs().get(0);
			}
			if (left.isDescendantOfBaseTable(nuwc2.getLeftOp().getAllColumnRefs().get(0).getAlias())) {
				leftFilterColumn2 = nuwc2.getLeftOp().getAllColumnRefs().get(0);
			}
			if (right.isDescendantOfBaseTable(nuwc2.getRightOp().getAllColumnRefs().get(0).getAlias())) {
				rightFilterColumn2 = nuwc2.getRightOp().getAllColumnRefs().get(0);
			}
			if (left.isDescendantOfBaseTable(nuwc2.getRightOp().getAllColumnRefs().get(0).getAlias())) {
				leftFilterColumn2 = nuwc2.getRightOp().getAllColumnRefs().get(0);
			}
		}
		// if(leftFilterColumn==null || rightFilterColumn==null){
		// return;
		// }
		SipInfo si = new SipInfo(p, join.getLeftOp().getAllColumnRefs().get(0), left);
		if (leftFilterColumn != null && rightFilterColumn != null) {
			si.addJoinCol(rightFilterColumn);
		}
		if (leftFilterColumn2 != null && rightFilterColumn2 != null) {
			si.addJoinCol(rightFilterColumn2);
		}
		boolean exists = false;
		SipInfoValue siv = new SipInfoValue(right, si.AnonymizeColumns());
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
		if (leftFilterColumn != null && rightFilterColumn != null) {
			si.addJoinCol(leftFilterColumn);
		}
		if (leftFilterColumn2 != null && rightFilterColumn2 != null) {
			si.addJoinCol(leftFilterColumn2);
		}
		siv = new SipInfoValue(left, si.AnonymizeColumns());
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


}

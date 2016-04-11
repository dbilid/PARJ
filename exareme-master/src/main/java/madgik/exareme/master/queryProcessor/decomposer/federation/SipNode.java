package madgik.exareme.master.queryProcessor.decomposer.federation;

import madgik.exareme.master.queryProcessor.decomposer.dag.Node;

public class SipNode {
	
	private Node n;
	private SipInfo si;
	public SipNode(Node n, SipInfo si) {
		super();
		this.n = n;
		this.si = si;
	}
	public Node getNode() {
		return n;
	}
	public void setNode(Node n) {
		this.n = n;
	}
	public SipInfo getSipInfo() {
		return si;
	}
	public void setSipInfo(SipInfo si) {
		this.si = si;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((n == null) ? 0 : n.hashCode());
		result = prime * result + ((si == null) ? 0 : si.hashCode());
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
		SipNode other = (SipNode) obj;
		if (n == null) {
			if (other.n != null)
				return false;
		} else if (n!=other.n)
			return false;
		if (si == null) {
			if (other.si != null)
				return false;
		} else if (!si.equals(other.si))
			return false;
		return true;
	}
	@Override
	public String toString() {
		return "SipNode [n=" + n + ", si=" + si + "]";
	}
	
	

	
}

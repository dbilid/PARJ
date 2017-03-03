package madgik.exareme.master.queryProcessor.decomposer.federation;

import madgik.exareme.master.queryProcessor.decomposer.dag.Node;
import madgik.exareme.master.queryProcessor.decomposer.query.Column;

public class CartesianSip {

	private Column c;
	private Node common;
	private Node other;

	public CartesianSip(Column c, Node common, Node other) {
		super();
		this.c = c;
		this.common = common;
		this.other = other;
	}

	public Column getC() {
		return c;
	}

	public void setC(Column c) {
		this.c = c;
	}

	public Node getCommon() {
		return common;
	}

	public Node getOther() {
		return other;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((c == null) ? 0 : c.hashCode());
		result = prime * result + ((common == null) ? 0 : common.hashCode());
		result = prime * result + ((other == null) ? 0 : other.hashCode());
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
		CartesianSip other = (CartesianSip) obj;
		if (c == null) {
			if (other.c != null)
				return false;
		} else if (!c.equals(other.c))
			return false;
		if (common == null) {
			if (other.common != null)
				return false;
		} else if (!common.getObject().equals(other.common.getObject()))
			return false;
		if (this.other == null) {
			if (other.other != null)
				return false;
		} else if (!this.other.getObject().equals(other.other.getObject()))
			return false;
		return true;
	}

}
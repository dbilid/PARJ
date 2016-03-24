package madgik.exareme.master.queryProcessor.decomposer.federation;


import madgik.exareme.master.queryProcessor.decomposer.query.Column;
import madgik.exareme.master.queryProcessor.decomposer.query.Projection;
import madgik.exareme.master.queryProcessor.decomposer.util.Util;

public class SipInfo {
	
	private Projection projection;
	private Column joinCol;
	private String joinNode;
	private int counter;
	private String name;
	
	public SipInfo(Projection projection, Column c, String node) {
		super();
		this.projection = projection;
		this.joinCol = c;
		this.joinNode=node;
		this.counter=0;
		this.name="siptable";
	}

	@Override
	public String toString() {
		return "SipInfo [projection=" + projection + ", joinCol=" + joinCol + ", joinNode=" + joinNode + " name=" + name+"]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((joinCol == null) ? 0 : joinCol.getHashID().asInt());
		result = prime * result + ((joinNode == null) ? 0 : joinNode.hashCode());
		result = prime * result + ((projection == null) ? 0 : projection.hashCode());
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
		SipInfo other = (SipInfo) obj;
		if (joinCol == null) {
			if (other.joinCol != null)
				return false;
		} else if (!joinCol.equals(joinCol))
			return false;
		if (joinNode == null) {
			if (other.joinNode != null)
				return false;
		} else if (!joinNode.equals(other.joinNode))
			return false;
		if (projection == null) {
			if (other.projection != null)
				return false;
		} else if (!projection.equals(other.projection))
			return false;
		return true;
	}

	public int getCounter() {
		return counter;
	}

	public void increaseCounter() {
		this.counter++;
	}
	
	public void resetCounter() {
		this.counter=0;
	}

	public String getJoinNode() {
		return joinNode;
	}

	public String getName() {
		return name;
	}

	public Column getJoinCol() {
		return joinCol;
	}
	
	
	

}

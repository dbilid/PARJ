package madgik.exareme.master.queryProcessor.decomposer.federation;


import java.util.ArrayList;
import java.util.List;

import madgik.exareme.master.queryProcessor.decomposer.dag.Node;
import madgik.exareme.master.queryProcessor.decomposer.query.Column;
import madgik.exareme.master.queryProcessor.decomposer.query.Output;
import madgik.exareme.master.queryProcessor.decomposer.query.Projection;
import madgik.exareme.master.queryProcessor.decomposer.util.Util;

public class SipInfo {
	
	private Projection projection;
	private Column joinCol;
	private Node joinNode;
	private int counter;
	private String name;
	
	public SipInfo(Projection projection, Column c, Node node) {
		super();
		try {
			this.projection = projection.clone();
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
		}
		this.joinCol = c;
		this.joinNode=node;
		this.counter=0;
		this.name="siptable"+ Util.createUniqueId();
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
		} else if (!joinNode.getObject().toString().equals(other.joinNode.getObject().toString()))
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
		return joinNode.getObject().toString();
	}

	public String getName() {
		return name;
	}

	public Column getJoinCol() {
		return joinCol;
	}

	public Projection getProjection() {
		return this.projection;
	}
	
	public List<Column> AnonymizeColumns(){
		//return a list with the anonymized columns
		//that is columns not coming from node
		int i=0;
		List<Column> result=new ArrayList<Column>();
		for(Output o:projection.getOperands()){
			for(Column c:o.getObject().getAllColumnRefs()){
				if(!joinNode.getDescendantBaseTables().contains(c.getAlias())){
					if(c instanceof Column){
						result.add(new Column(c.getAlias(), c.getName()));
					}
					else{
						result.add(c);
					}
					o.getObject().changeColumn(c, new Column("AnonCol"+i, "AnonCol"+i));
				}
			}
		}
		return result;
	}
	

}

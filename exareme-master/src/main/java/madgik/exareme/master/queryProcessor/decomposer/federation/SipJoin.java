package madgik.exareme.master.queryProcessor.decomposer.federation;

import madgik.exareme.master.queryProcessor.decomposer.query.NonUnaryWhereCondition;

public class SipJoin {
	
	private int joinNumber;
	private NonUnaryWhereCondition join;
	private String sipName;
	boolean deleteOnTableInsert;
	
	
	
	public SipJoin(int joinNumber, NonUnaryWhereCondition join, String sipName, boolean d) {
		super();
		this.joinNumber = joinNumber;
		this.join = join;
		this.sipName = sipName;
		this.deleteOnTableInsert=d;
	}



	public String getSipName() {
		return sipName;
	}



	public boolean isDeleteOnTableInsert() {
		return deleteOnTableInsert;
	}



	@Override
	public String toString() {
		return "SipJoin [joinNumber=" + joinNumber + ", join=" + join
				+ ", sipName=" + sipName + ", deleteOnTableInsert="
				+ deleteOnTableInsert + "]";
	}



	public NonUnaryWhereCondition getBwc() {
		return join;
	}



	public int getNumber() {
		return this.joinNumber;
	}
	
	

}

/**
 * Copyright MaDgIK Group 2010 - 2015.
 */
package madgik.exareme.master.queryProcessor.decomposer.query;



public class Table {

	private int name;
	private int alias;
	private int replica;
	private int dictionary;
	private boolean inverse;
	private static final org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(Table.class);

	

	public Table(int n, int a) {
		this.name = n;
		this.alias = a;
		this.inverse=false;
		this.replica=0;
		this.dictionary=0;
	}


	@Override
	public String toString() {
		String result;
		if(dictionary>0){
			return "dictionary d"+dictionary;
		}
		if(inverse){
			result= "memorywrapperinvprop"+name;
		}
		else{
		 result= "memorywrapperprop"+name;
		}
		if(replica>0){
			result+="_"+replica;
		}
		result+=" "+"alias"+alias;
		return result;
	}

	public String dotPrint() {
		return this.toString();
	}


	/**
	 * @return the name
	 */
	public int getName() {
		return name;
	}

	/**
	 * @param name
	 *            the name to set
	 */
	public void setName(int name) {
		this.name = name;
	}

	/**
	 * @return the alias
	 */
	public int getAlias() {
		return alias;
	}

	/**
	 * @param alias
	 *            the alias to set
	 */
	public void setAlias(int alias) {
		this.alias = alias;
	}


	public boolean isInverse() {
		return inverse;
	}


	public void setInverse(boolean inverse) {
		this.inverse = inverse;
	}


	public void setReplica(int replica) {
		this.replica = replica;
	}


	public void setDictionary(int dictionary) {
		this.dictionary = dictionary;
	}

	
	
	
}

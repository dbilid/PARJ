/**
 * Copyright MaDgIK Group 2010 - 2015.
 */
package madgik.exareme.master.queryProcessor.decomposer.query;



public class Table {

	private int name;
	private int alias;
	private boolean inverse;
	private static final org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(Table.class);

	

	public Table(int n, int a) {
		this.name = n;
		this.alias = a;
		this.inverse=false;
	}


	@Override
	public String toString() {
		if(inverse){
			return "memorywrapperinvprop"+name+" "+"alias"+alias;
		}
		else{
			return "memorywrapperprop"+name+" "+"alias"+alias;
		}
		
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

	
	
}

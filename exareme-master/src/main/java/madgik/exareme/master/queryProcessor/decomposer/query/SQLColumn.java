/**
 * Copyright MaDgIK Group 2010 - 2015.
 */
package madgik.exareme.master.queryProcessor.decomposer.query;

import java.util.List;

import com.google.common.hash.HashCode;


public class SQLColumn implements Operand {

	private String tableAlias;
	private String name;

	

	public SQLColumn(String alias, String name) {
		this.tableAlias = alias;
		this.name = name;
	}

	
	public void setColumnName(String columnName) {
		this.name = columnName;
	}




	@Override
	public String toString() {
		return tableAlias + "."+name ;
			
	}

	@Override
	public List<Column> getAllColumnRefs() {
		
		return null;
	}

	@Override
	public void changeColumn(Column oldCol, Column newCol) {
		///if (this.isSubject==oldCol.isSubject && this.tableAlias==oldCol.tableAlias) {
		//	this.isSubject = newCol.isSubject;
		//	this.tableAlias = newCol.tableAlias;
		//}
	}

	@Override
	public SQLColumn clone() throws CloneNotSupportedException {
		SQLColumn cloned = (SQLColumn) super.clone();
		return cloned;
	}

	public String getName() {
		return this.name;
	}

	public String getAlias() {
		return this.tableAlias;
	}

	public void setName(String isSubject) {
		this.name = isSubject;
	}

	public void setAlias(String tablename) {
		this.tableAlias = tablename;

	}

	public String getColumnName() {
		return this.name;
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((name == null) ? 0 : name.hashCode());
		result = prime * result
				+ ((tableAlias == null) ? 0 : tableAlias.hashCode());
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
		SQLColumn other = (SQLColumn) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (tableAlias == null) {
			if (other.tableAlias != null)
				return false;
		} else if (!tableAlias.equals(other.tableAlias))
			return false;
		return true;
	}


	@Override
	public HashCode getHashID() {
		// TODO Auto-generated method stub
		return null;
	}

	
	

}

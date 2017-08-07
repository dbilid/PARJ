/**
 * Copyright MaDgIK Group 2010 - 2015.
 */
package madgik.exareme.master.queryProcessor.decomposer.query;

import java.util.ArrayList;
import java.util.List;

import com.google.common.hash.HashCode;


public class Column implements Operand {

	private int tableAlias;
	private boolean isSubject;

	

	public Column(int alias, boolean name) {
		this.tableAlias = alias;
		this.isSubject = name;
	}

	
	public void setColumnName(boolean columnName) {
		this.isSubject = columnName;
	}




	@Override
	public String toString() {
		String table = "alias"+tableAlias + ".";;
			if(isSubject){
				table+="first";
			}
			else{
				table+="second";
			}
		

		return table;
	}

	@Override
	public List<Column> getAllColumnRefs() {
		List<Column> res = new ArrayList<Column>();
		res.add(this);
		return res;
	}

	@Override
	public void changeColumn(Column oldCol, Column newCol) {
		if (this.isSubject==oldCol.isSubject && this.tableAlias==oldCol.tableAlias) {
			this.isSubject = newCol.isSubject;
			this.tableAlias = newCol.tableAlias;
		}
	}

	@Override
	public Column clone() throws CloneNotSupportedException {
		Column cloned = (Column) super.clone();
		return cloned;
	}

	public boolean getName() {
		return this.isSubject;
	}

	public int getAlias() {
		return this.tableAlias;
	}

	public void setName(boolean isSubject) {
		this.isSubject = isSubject;
	}

	public void setAlias(int tablename) {
		this.tableAlias = tablename;

	}

	public boolean getColumnName() {
		return this.isSubject;
	}


	@Override
	public HashCode getHashID() {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (isSubject ? 1231 : 1237);
		result = prime * result + tableAlias;
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
		Column other = (Column) obj;
		if (isSubject != other.isSubject)
			return false;
		if (tableAlias != other.tableAlias)
			return false;
		return true;
	}

	

}

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package madgik.exareme.master.queryProcessor.decomposer.query;

/**
 * @author dimitris
 */
public class ColumnOrderBy extends Column {

	public boolean isAsc = true;

	
	public ColumnOrderBy(int s1, boolean s2, boolean asc) {
		super(s1, s2);
		this.isAsc = asc;
	}

	@Override
	public String toString() {
		String order = "";
		if (!this.isAsc) {
			order = " DESC";
		}
			return getAlias() + "." + getName() + order;
		
	}
}

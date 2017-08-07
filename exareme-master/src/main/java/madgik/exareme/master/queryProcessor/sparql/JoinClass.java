package madgik.exareme.master.queryProcessor.sparql;

import java.util.HashSet;
import java.util.Set;

import madgik.exareme.master.queryProcessor.decomposer.query.Column;
import madgik.exareme.master.queryProcessor.decomposer.query.Operand;

public class JoinClass {

	private Set<Column> cols;

	public JoinClass() {
		super();
		this.cols = new HashSet<Column>();
	}

	public JoinClass(Column c) {
		super();
		this.cols = new HashSet<Column>();
		this.cols.add(c);
	}

	public void merge(JoinClass other) {
		this.cols.addAll(other.cols);
	}

	public Column getFirstColumn() {
		return cols.iterator().next();
	}
	
	public void addColumn(Column c){
		cols.add(c);
	}
	
	public Set<Column> getColumns(){
		return cols;
	}

}

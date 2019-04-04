package madgik.exareme.master.queryProcessor.analyzer.stat;

public class TableSize implements Comparable<TableSize>{
	
	double size;
	int table;
	public TableSize(double size, int table) {
		super();
		this.size = size;
		this.table = table;
	}
	public Double getSize() {
		return size;
	}
	public void setSize(double size) {
		this.size = size;
	}
	public int getTable() {
		return table;
	}
	public void setTable(int table) {
		this.table = table;
	}
	@Override
	public int compareTo(TableSize o) {
		return Double.compare(size, o.size);
	}
	
	

}



package madgik.exareme.master.queryProcessor.decomposer.query;

public class RepartitionColumn extends Column{
	
	private boolean to1;
	

	public RepartitionColumn() {
		super();
	}

	public RepartitionColumn(Column column) {
		super(column);
	}

	public RepartitionColumn(String alias, String name, String base) {
		super(alias, name, base);
	}

	public RepartitionColumn(String alias, String name) {
		super(alias, name);
	}
	
	

}

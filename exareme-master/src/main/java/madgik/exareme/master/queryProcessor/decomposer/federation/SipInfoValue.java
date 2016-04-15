package madgik.exareme.master.queryProcessor.decomposer.federation;

import java.util.List;

import madgik.exareme.master.queryProcessor.decomposer.dag.Node;
import madgik.exareme.master.queryProcessor.decomposer.query.Column;

public class SipInfoValue {
	
	private Node node;
	private List<Column> outputs;
	
	public SipInfoValue(Node node, List<Column> outputs) {
		super();
		this.node = node;
		this.outputs = outputs;
	}

	public Node getNode() {
		return node;
	}

	public List<Column> getOutputs() {
		return outputs;
	}
	
	
	
	
	
	

}

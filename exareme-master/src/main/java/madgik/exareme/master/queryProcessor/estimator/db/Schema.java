package madgik.exareme.master.queryProcessor.estimator.db;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

import java.util.HashMap;
import java.util.Map;

import madgik.exareme.master.queryProcessor.analyzer.stat.JoinCardinalities;

/**
 * @author jim
 */
public class Schema {

	private String schemaName;
	private Map<Integer, RelInfo> tableIndex;
	private JoinCardinalities cards;

	/* constructor */
	public Schema(String schemaName, Map<Integer, RelInfo> relIndex) {
		this.schemaName = schemaName;
		this.tableIndex = relIndex;
	}

	/* copy constructor */
	public Schema(Schema schema) {
		this.schemaName = schema.getSchemaName();
		this.tableIndex = new HashMap<Integer, RelInfo>();

		for (Map.Entry<Integer, RelInfo> entry : schema.tableIndex.entrySet()) {
			this.tableIndex.put(entry.getKey(), new RelInfo(entry.getValue()));
		}

	}

	/* getters and setters */
	public String getSchemaName() {
		return schemaName;
	}

	public Map<Integer, RelInfo> getTableIndex() {
		return tableIndex;
	}

	/* interface methods */

	/* standard methods */
	@Override
	public String toString() {
		return "Schema{" + "schemaName=" + schemaName + ", tableIndex=" + tableIndex + '}';
	}

	public JoinCardinalities getCards() {
		return cards;
	}

	public void setCards(JoinCardinalities cards) {
		this.cards = cards;
	}
	
	

}

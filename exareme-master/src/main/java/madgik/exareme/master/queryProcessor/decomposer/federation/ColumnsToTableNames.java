/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package madgik.exareme.master.queryProcessor.decomposer.federation;

import madgik.exareme.master.queryProcessor.decomposer.query.Column;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author dimitris
 */
public class ColumnsToTableNames<T> extends HashMap<String, T> {
	
	private Map<String, T> tableToT;

    public ColumnsToTableNames() {
        super();
        tableToT=new HashMap<String, T>();
    }

    public void putColumnInTable(Column c, T t) {
        this.put(c.getAlias() + ":" + c.getName(), t);
    }

    public T getTablenameForColumn(Column c) {
    	if(tableToT.containsKey(c.getAlias())){
    		return tableToT.get(c.getAlias());
    	}
        return this.get(c.getAlias() + ":" + c.getName());
    }

    public boolean containsColumn(Column c) {
        return this.containsKey(c.getAlias() + ":" + c.getName()) ||
        		tableToT.containsKey(c.getAlias());
    }

    public List<Column> getAllColumns() {
        List<Column> result = new ArrayList<Column>();
        for (String c : this.keySet()) {
            result.add(new Column(c.split(":")[0], c.split(":")[1]));
        }
        return result;
    }

    void changeColumns(T toChange, T join) {
        for (String s : this.keySet()) {
            if (get(s).equals(toChange)) {
                put(s, join);
            }
        }
        for (String s : this.tableToT.keySet()) {
            if (tableToT.get(s).equals(toChange)) {
            	tableToT.put(s, join);
            }
        }
    }
    
    public void addTable(String s, T t){
    	this.tableToT.put(s, t);
    }
    
}

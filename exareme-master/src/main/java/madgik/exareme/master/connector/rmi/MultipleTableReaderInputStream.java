package madgik.exareme.master.connector.rmi;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import madgik.exareme.common.schema.ResultTable;
import madgik.exareme.master.client.AdpDBClient;
import madgik.exareme.worker.art.executionEngine.dynamicExecutionEngine.event.terminated.OperatorGroupTerminatedEventHandler;

public class MultipleTableReaderInputStream extends FilterInputStream {

	private AdpDBClient dbClient;
	private Map<String, String> tableNamesToOutputs;
	private List<ResultTable> tables;
	private boolean first;
	private static final Logger log = Logger.getLogger(OperatorGroupTerminatedEventHandler.class);
	

	public MultipleTableReaderInputStream(List<ResultTable> result) {
		super(null);
		this.tables = result;
		first = true;
	}

	@Override
	public int read() throws IOException {
		int result = -1;
		if (in != null)
			result=super.read();
		if (result == -1) {
			while (result == -1 && !tableNamesToOutputs.isEmpty()) {
				ResultTable nextTable = getNextTable();
				InputStream newIn = dbClient.readTable(nextTable, first, tableNamesToOutputs.get(nextTable.getName()));
				first = false;
				tableNamesToOutputs.remove(nextTable.getName());
				result = newIn.read();
				if(in!=null)
					in.close();
				in = newIn;
			}
		}
		return result;
	}

	private ResultTable getNextTable() {
		ResultTable result = null;
		while (result == null) {
			synchronized (ResultTable.allTables) {
				for (ResultTable rt : ResultTable.allTables) {
					if (rt.isReady() && tableNamesToOutputs.containsKey(rt.getName())) {
						result = rt;
						ResultTable.allTables.remove(rt);
						System.out.println("nextttt"+ResultTable.allTables.size());
						break;
					}
				}
				if (result == null) {
					try {
						ResultTable.allTables.wait();
						System.out.println("duh ");
					} catch (InterruptedException e) {
					}
				}
			}
		}
		log.debug("Table ready to be sent:"+result);
		return result;
	}

	@Override
	public int read(byte[] b) throws IOException {
		int result = -1;
		if (in != null)
			result=super.read(b);
		if (result == -1) {
			while (result == -1 && !tableNamesToOutputs.isEmpty()) {
				ResultTable nextTable = getNextTable();
				InputStream newIn = dbClient.readTable(nextTable, first, tableNamesToOutputs.get(nextTable.getName()));
				first = false;
				tableNamesToOutputs.remove(nextTable.getName());
				result = newIn.read(b);
				if(in!=null)
					in.close();
				in = newIn;
			}
		}
		return result;
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		int result = -1;
		if (in != null)
			result=super.read(b, off, len);
		if (result == -1) {
			while (result == -1 && !tableNamesToOutputs.isEmpty()) {
				ResultTable nextTable = getNextTable();
				InputStream newIn = dbClient.readTable(nextTable, first, tableNamesToOutputs.get(nextTable.getName()));
				first = false;
				tableNamesToOutputs.remove(nextTable.getName());
				result = newIn.read(b, off, len);
				if(in!=null)
					in.close();
				in = newIn;
				
			}
		}
		return result;
	}

	@Override
	public boolean markSupported() {
		return false;
	}

	public void setDbClient(AdpDBClient dbClient) {
		this.dbClient = dbClient;
	}

	public void setTableNames(Map<String, String> tableNames) {
		this.tableNamesToOutputs = tableNames;
	}

	public void setTables(List<ResultTable> tables) {
		this.tables = tables;
	}

	@Override
	public int available() throws IOException {
		if(in==null){
			return 0;
		}
		else{
			return super.available();
		}
	}
	
	

}

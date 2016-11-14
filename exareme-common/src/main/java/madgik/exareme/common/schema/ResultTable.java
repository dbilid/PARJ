package madgik.exareme.common.schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ResultTable implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String db;
	private String name;
	private String ip;
	private boolean isReady;
	public static List<ResultTable> allTables=new ArrayList<ResultTable>();
	
	public ResultTable(String db, String name) {
		super();
		this.db = db;
		this.name = name;
		isReady=false;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public boolean isReady() {
		return isReady;
	}

	public void setReady(boolean isReady) {
		this.isReady = isReady;
	}

	public String getName() {
		return name;
	}

	public String getDb() {
		return db;
	}


}

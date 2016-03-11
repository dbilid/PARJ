/**
	 * Copyright MaDgIK Group 2010 - 2015.
	 */
package madgik.exareme.master.connector.local;

import com.google.gson.Gson;
import madgik.exareme.common.schema.Partition;
import madgik.exareme.common.schema.PhysicalTable;
import madgik.exareme.master.client.AdpDBClientProperties;
import madgik.exareme.master.connector.AdpDBConnectorUtil;
import madgik.exareme.master.registry.Registry;
import org.apache.log4j.Logger;

import java.io.PipedOutputStream;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

/**
 * @author heraldkllapi
 * @author dimitris
 */
public class AdpDBQueryExecutorThread extends Thread {
	private final String queryString;
	private final Map<String, Object> alsoIncludeProps;
	private final AdpDBClientProperties props;
	private final PipedOutputStream out;
	private final Set<String> referencedTables;
	private Logger log = Logger.getLogger(AdpDBQueryExecutorThread.class);

	public AdpDBQueryExecutorThread(String q, Map<String, Object> alsoIncludeProps, AdpDBClientProperties props,
			Set<String> referencedTables, PipedOutputStream out) {
		this.queryString = q;
		this.alsoIncludeProps = alsoIncludeProps;
		this.props = props;
		this.out = out;
		this.referencedTables=referencedTables;
	}

	@Override
	public void run() {
		try {

				AdpDBConnectorUtil.executeLocalQuery(queryString, referencedTables, 0, props.getDatabase(), alsoIncludeProps, out);
			
		} catch (Exception e) {
			log.error("Cannot get results", e);
		} finally {
			try {
				out.flush();
				out.close();
			} catch (Exception e) {
				log.error("Cannot close output", e);
			}
		}
	}
}
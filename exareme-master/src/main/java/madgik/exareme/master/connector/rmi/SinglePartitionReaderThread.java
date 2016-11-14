package madgik.exareme.master.connector.rmi;

import java.io.PipedOutputStream;
import java.util.ArrayList;
import java.util.Map;

import org.apache.log4j.Logger;

import com.google.gson.Gson;

import madgik.exareme.common.schema.Partition;
import madgik.exareme.common.schema.PhysicalTable;
import madgik.exareme.common.schema.ResultTable;
import madgik.exareme.master.client.AdpDBClientProperties;
import madgik.exareme.master.connector.AdpDBConnectorUtil;
import madgik.exareme.master.registry.Registry;

public class SinglePartitionReaderThread extends Thread {
	private final ResultTable rTable;
	private final Map<String, Object> alsoIncludeProps;
	private final AdpDBClientProperties props;
	private final PipedOutputStream out;
	private String output;
	private Logger log = Logger.getLogger(SinglePartitionReaderThread.class);

	public SinglePartitionReaderThread(ResultTable table, Map<String, Object> alsoIncludeProps,
			AdpDBClientProperties props, PipedOutputStream out, String output) {
		this.rTable = table;
		this.alsoIncludeProps = alsoIncludeProps;
		this.props = props;
		this.out = out;
		this.output = output;
	}

	@Override
	public void run() {
		String error = null;
		try {
			log.info("Reading table " + rTable.getName());

			AdpDBConnectorUtil.readSingleTablePart(rTable, alsoIncludeProps, out, output);
		} catch (Exception e) {
			error = e.getMessage();
		} finally {
			try {
				if (error != null) {
					log.error("Cannot get results: " + error);
					ArrayList<Object> errors = (ArrayList<Object>) alsoIncludeProps.get("errors");
					errors.add("Cannot read result table " + rTable.getName() + ": " + error);
					Gson g = new Gson();
					out.write((g.toJson(alsoIncludeProps) + "\n").getBytes());
				}
				out.flush();
				out.close();
			} catch (Exception e) {
				log.error("Cannot close output", e);
			}
		}
	}
}

/**
 * Copyright MaDgIK Group 2010 - 2015.
 */
package madgik.exareme.master.connector.rmi;

import madgik.exareme.common.schema.ResultTable;
import madgik.exareme.master.client.AdpDBClientProperties;
import madgik.exareme.master.connector.AdpDBConnector;
import org.apache.log4j.Logger;

import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.rmi.RemoteException;
import java.rmi.ServerException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author heraldkllapi
 */
public class RmiAdpDBConnector implements AdpDBConnector {
    private static final Logger log = Logger.getLogger(RmiAdpDBConnector.class);
    private static ExecutorService pool = Executors.newFixedThreadPool(100);

    
    @Override 
    public InputStream readTable(String tableName, Map<String, Object> alsoIncludeProps,
            AdpDBClientProperties props, String output) throws RemoteException {
            try {
                PipedOutputStream out = new PipedOutputStream();
                pool.submit(new AdpDBNetReaderThread(tableName, alsoIncludeProps, props, out, output));
                log.debug("Net Reader submitted.");
                return new PipedInputStream(out);
            } catch (Exception e) {
                throw new ServerException("Cannot read table: " + tableName, e);
            }
        }


	@Override
	public InputStream readTable(ResultTable nextTable, HashMap<String, Object> additionalProps,
			AdpDBClientProperties properties, String output) throws RemoteException {
        try {
            PipedOutputStream out = new PipedOutputStream();
            pool.submit(new SinglePartitionReaderThread(nextTable, additionalProps, properties, out, output));
            log.debug("Net Reader submitted.");
            return new PipedInputStream(out);
        } catch (Exception e) {
            throw new ServerException("Cannot read table: " + nextTable.getName(), e);
        }
    }


}

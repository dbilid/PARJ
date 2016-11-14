/**
 * Copyright MaDgIK Group 2010 - 2015.
 */
package madgik.exareme.master.connector;

import madgik.exareme.common.schema.ResultTable;
import madgik.exareme.master.client.AdpDBClientProperties;

import java.io.InputStream;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author heraldkllapi
 */
public interface AdpDBConnector {

    // Returns a stream of json records.
	InputStream readTable(String tblName, Map<String, Object> additionalProps, AdpDBClientProperties properties,
			String output) throws RemoteException;

	InputStream readTable(ResultTable nextTable, HashMap<String, Object> additionalProps,
			AdpDBClientProperties properties, String output) throws RemoteException;




}

/**
 * Copyright MaDgIK Group 2010 - 2015.
 */
package madgik.exareme.master.gateway;

import madgik.exareme.master.db.DBManager;
import madgik.exareme.master.gateway.async.HttpAsyncExaremeGateway;
import org.apache.log4j.Logger;

/**
 * @author alex
 * @since 0.1
 */
public class ExaremeGatewayFactory {
	private static Logger log = Logger.getLogger(ExaremeGateway.class);

	public static ExaremeGateway createHttpServer(DBManager manager) throws Exception {
		return new HttpAsyncExaremeGateway(manager);
	}

}

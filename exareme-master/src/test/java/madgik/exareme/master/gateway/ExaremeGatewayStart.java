package madgik.exareme.master.gateway;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import madgik.exareme.master.db.DBManager;

import java.rmi.RemoteException;

/**
 * @author alex
 */
public class ExaremeGatewayStart {

    public static void main(String[] args) throws Exception {
        Logger.getRootLogger().setLevel(Level.ALL);


        final ExaremeGateway gateway =
            ExaremeGatewayFactory.createHttpServer(new DBManager());

        gateway.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override public void run() {
                gateway.stop();
               
            }
        });
    }
}

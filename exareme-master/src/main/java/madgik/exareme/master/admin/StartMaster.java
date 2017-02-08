package madgik.exareme.master.admin;

import madgik.exareme.master.dbmanager.DBManager;
import madgik.exareme.master.gateway.ExaremeGateway;
import madgik.exareme.master.gateway.ExaremeGatewayFactory;
import madgik.exareme.utils.properties.AdpProperties;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.Map;

public class StartMaster {

    private static final Logger log = Logger.getLogger(StartMaster.class);
    //private static ArtManager manager = null;
    //private static AdpDBManager dbManager = null;
    private static ExaremeGateway gateway = null;

    private StartMaster() {
    }

    public static void main(String[] args) throws Exception {
        log.info("Starting up master.");

       
        for (Map.Entry<String, String> entry : System.getenv().entrySet()) {
            log.info(entry.getKey() + " = " + entry.getValue());
        }

        String logLevel = AdpProperties.getArtProps().getString("art.log.level");
        Logger.getRootLogger().setLevel(Level.toLevel(logLevel));



        log.info("Starting gateway ...");
        gateway = ExaremeGatewayFactory.createHttpServer(new DBManager());
        gateway.start();
        log.debug("Gateway Started.");

        log.info("Master node started.");
    }
}

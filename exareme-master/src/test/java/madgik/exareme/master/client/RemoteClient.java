package madgik.exareme.master.client;

import madgik.exareme.master.app.cluster.ExaremeCluster;
import madgik.exareme.master.app.cluster.ExaremeClusterFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by christos on 12/4/2016.
 */
public class RemoteClient {

    private static final Logger log = Logger.getLogger(RemoteClient.class);
    private int registryPort = 1099;
    private int dtPort = 8088;
    private int nclients = 1;
    private int nworkers = 2;
    private String dbPathName;
    private String[] load_script1;
    private String[] load_script2;
    private String[] query_script;
    private boolean cache = true;

    private class RunnableAdpDBClient implements Runnable {
        private final int id;
        private AdpDBClient client;

        public RunnableAdpDBClient(final int id, final AdpDBClient client) {
            this.id = id;
            this.client = client;
        }

        public void run() {
            try {
                AdpDBClientQueryStatus queryStatus =
                        client.query("load_" + String.valueOf(id), load_script1[id]);
                while (queryStatus.hasFinished() == false && queryStatus.hasError() == false) {
                    try {
                        Thread.sleep(10 * 1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                if (queryStatus.hasError()) {
                    log.error("Exception occured..." + queryStatus.getError());
                }
                Assert.assertTrue(queryStatus != null);
                Assert.assertFalse(queryStatus.hasError());
//
                queryStatus = client.query("index_" + String.valueOf(id), load_script2[id]);
                while (queryStatus.hasFinished() == false && queryStatus.hasError() == false) {
                    try {
                        Thread.sleep(10 * 1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                if (queryStatus.hasError()) {
                    log.error("Exception occured..." + queryStatus.getError());
                }
                Assert.assertTrue(queryStatus != null);
                Assert.assertFalse(queryStatus.hasError());

                queryStatus = client.query("query_" + String.valueOf(id), query_script[id]);
                while (queryStatus.hasFinished() == false && queryStatus.hasError() == false) {
                    try {
                        Thread.sleep(10 * 1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                if (queryStatus.hasError()) {
                    log.error("Exception occured..." + queryStatus.getError());
                }
                Assert.assertTrue(queryStatus != null);
                Assert.assertFalse(queryStatus.hasError());


            } catch (RemoteException e) {
                log.error("Error occurred ( " + String.valueOf(id) + ")!", e);
            } catch (IOException e) {
                log.error("Error occurred while reading results", e);
            }
        }
    }

    public RemoteClient() {

    }

    @Before
    public void setUp() throws Exception {
        Logger.getRootLogger().setLevel(Level.DEBUG);
//        Thread.sleep(1000*20);
        log.debug("---- SETUP ----");
        log.debug(TestAdpDBClient.class.getResource("load_emp_template.sql") == null);
        // load & format scripts
        File loadFile =
                new File(TestAdpDBClient.class.getResource("load_emp_template.sql").getFile());
        load_script1 = new String[nclients];
        load_script2 = new String[nclients];
        query_script = new String[nclients];

        for (int i = 0; i < nclients; i++) {

            StringBuilder script = new StringBuilder();
            script.append("distributed create table lessons as external\n")
                    .append("select * from(mysql h:localhost port:3306 u:root p:031289 db:university select * from lessons);\n");
//                    .append("select * from(file '" + TestAdpDBClient.class.getResource("subject.tsv").getFile() + "' dialect:tsv delimiter:, header:t);\n\n");

            System.out.println("script is " + script.toString());
            this.load_script1[i] = script.toString();


            script = new StringBuilder();
            script.append("distributed create temporary table tmp_lessons to 2 on id\n")
                    .append(" as direct \n")
                    .append("select * from lessons;\n")
                    .append("distributed create temporary table mod_lessons \n")
                    .append(" as direct \n")
                    .append("select * from tmp_lessons;\n\n")
                    .append("distributed create table mod_lessons_v2 as \n" +
                            "select * from mod_lessons\n;");
            this.load_script2[i] = script.toString();

            script = new StringBuilder();
            script.append("distributed create table ").append("query_lessons1 to 3 on name1 as direct \n")
//                    .append(" as external select * from range(5);")
                    .append("select l1.`id` as id, l1.`name` as name1, l2.`name` as name2 from mod_lessons_v2 l1, mod_lessons_v2 l2 where l1.id=l2.id")
                    .append(";");
            this.query_script[i] = script.toString();

        }
        log.debug("Scripts successfully formatted.");

        this.dbPathName = "/home/christos/database";
        new File(dbPathName).mkdirs();
        log.debug("Database created.");

        log.debug("---- SETUP ----");
    }

    @Test
    public void testAdpDBClient() throws Exception {
        log.debug("---- TEST ----");

//        AdpDBClientProperties properties = new AdpDBClientProperties(dbPathName);
        AdpDBClientProperties properties = new AdpDBClientProperties(dbPathName, "", "", cache, false, false, -1, 10);

        ExaremeCluster miniCluster =
                ExaremeClusterFactory.createMiniCluster(registryPort, dtPort, nworkers);
        log.debug("Mini cluster created.");

        miniCluster.start();

        log.debug("Mini cluster started.");

        ExecutorService executorService = Executors.newFixedThreadPool(nclients);
        log.debug("Executor service created.");

        for (int i = 0; i < nclients; i++) {
            executorService.execute(new RunnableAdpDBClient(i,
                    miniCluster.getExaremeClusterClient(properties)));
        }
        log.debug("Clients created.(" + nclients + ")");

        executorService.shutdown();
        log.debug("Executor service shutdown.");

        try {
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            log.error("Unable shutdown executor.", e);
        }

        miniCluster.stop(true);
        log.debug("Mini cluster stopped.");

        log.debug("---- TEST ----");
    }

    @After
    public void tearDown() throws Exception {
        log.debug("---- CLEAN ----");
//        FileUtils.deleteDirectory(new File(dbPathName));
        log.debug("---- CLEAN ----");
//        Thread.sleep(1000 * 20);

    }

}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package madgik.exareme.master.queryProcessor.analyzer.dbstats;

import com.google.gson.Gson;

import madgik.exareme.master.queryProcessor.analyzer.stat.ExternalStat;
import madgik.exareme.master.queryProcessor.analyzer.stat.Stat;
import madgik.exareme.master.queryProcessor.analyzer.stat.Table;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Map;
import java.util.Set;

/**
 * @author jim
 */
public class ExternalGatherer {

    private Map<String, Table> schema;
    private String tblName;
    private String schName;
    private Connection conn;
    Set<String> attrs;

    public ExternalGatherer(Connection c, String tblName, String sch, Set<String> attrs) {
        this.conn=c;
        this.tblName = tblName;
        schName=sch;
    }

    public Map<String, Table> gather(String dbpath) throws Exception {
    	ExternalStat stat = new ExternalStat(conn, tblName, schName);
        schema = stat.extractStats();

        //

	/*	for (Entry<String, Table> e : schema.entrySet()) {

			System.out.println("TABLE: " + e.getKey() + " TUPLES: "
					+ e.getValue().getNumberOfTuples());
			for (Entry<String, Column> ee : schema.get(e.getKey())
					.getColumnMap().entrySet()) {

				int s = 0;
				for (int i : ee.getValue().getDiffValFreqMap().values())
					s += i;

				System.out.println("COLUMN: " + ee.getKey() + " TUPLES: " + s);
			}

		}*/

        //dataToJson(dbName, dbpath);
        return schema;

    }


    private void dataToJson(String filename, String dbpath) throws Exception {

        Gson gson = new Gson();
        String jsonStr = gson.toJson(schema);

        PrintWriter writer = new PrintWriter(dbpath + filename + ".json", "UTF-8");
        writer.println(jsonStr);
        writer.close();

    }
}

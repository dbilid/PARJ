package madgik.exareme.master.queryProcessor.decomposer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import madgik.exareme.master.queryProcessor.decomposer.dag.NodeHashValues;
import madgik.exareme.master.queryProcessor.decomposer.federation.DBInfoReaderDB;
import madgik.exareme.master.queryProcessor.decomposer.federation.NamesToAliases;
import madgik.exareme.master.queryProcessor.decomposer.federation.QueryDecomposer;
import madgik.exareme.master.queryProcessor.decomposer.query.SQLQuery;
import madgik.exareme.master.queryProcessor.decomposer.query.SQLQueryParser;
import madgik.exareme.master.queryProcessor.estimator.NodeSelectivityEstimator;

public class DemoDAG {

	public static final String queryExample="SELECT *\n" +
			"FROM (\n" +
			"SELECT \n" +
			"   7 AS \"wellboreQuestType\", NULL AS \"wellboreLang\", CAST(QVIEW1.\"wlbWellboreName\" AS VARCHAR(10485760)) AS \"wellbore\", \n" +
			"   5 AS \"lenghtMQuestType\", NULL AS \"lenghtMLang\", CAST(QVIEW4.\"lsuCoreLenght\" AS VARCHAR(10485760)) AS \"lenghtM\", \n" +
			"   7 AS \"companyQuestType\", NULL AS \"companyLang\", CAST(QVIEW2.\"wlbDrillingOperator\" AS VARCHAR(10485760)) AS \"company\", \n" +
			"   4 AS \"yearQuestType\", NULL AS \"yearLang\", CAST(QVIEW2.\"wlbCompletionYear\" AS VARCHAR(10485760)) AS \"year\"\n" +
			" FROM \n" +
			"\"wellbore_development_all\" QVIEW1,\n" +
			"\"wellbore_exploration_all\" QVIEW2,\n" +
			"\"company\" QVIEW3,\n" +
			"\"strat_litho_wellbore_core\" QVIEW4,\n" +
			"\"wellbore_npdid_overview\" QVIEW5\n" +
			"WHERE \n" +
			"QVIEW1.\"wlbWellboreName\" IS NOT NULL AND\n" +
			"QVIEW1.\"wlbNpdidWellbore\" IS NOT NULL AND\n" +
			"(QVIEW1.\"wlbNpdidWellbore\" = QVIEW2.\"wlbNpdidWellbore\") AND\n" +
			"QVIEW2.\"wlbCompletionYear\" IS NOT NULL AND\n" +
			"(QVIEW2.\"wlbDrillingOperator\" = QVIEW3.\"cmpLongName\") AND\n" +
			"QVIEW2.\"wlbDrillingOperator\" IS NOT NULL AND\n" +
			"QVIEW3.\"cmpNpdidCompany\" IS NOT NULL AND\n" +
			"(QVIEW1.\"wlbNpdidWellbore\" = QVIEW4.\"wlbNpdidWellbore\") AND\n" +
			"(QVIEW1.\"wlbNpdidWellbore\" = QVIEW5.\"wlbNpdidWellbore\") AND\n" +
			"QVIEW4.\"lsuNpdidLithoStrat\" IS NOT NULL AND\n" +
			"QVIEW4.\"lsuCoreLenght\" IS NOT NULL AND\n" +
			"((QVIEW4.\"lsuCoreLenght\" > 50) AND (QVIEW2.\"wlbCompletionYear\" >= 2008))\n" +
			"UNION\n" +
			"SELECT \n" +
			"   7 AS \"wellboreQuestType\", NULL AS \"wellboreLang\", CAST(QVIEW1.\"wlbWellboreName\" AS VARCHAR(10485760)) AS \"wellbore\", \n" +
			"   5 AS \"lenghtMQuestType\", NULL AS \"lenghtMLang\", CAST(QVIEW6.\"wlbTotalCoreLength\" AS VARCHAR(10485760)) AS \"lenghtM\", \n" +
			"   7 AS \"companyQuestType\", NULL AS \"companyLang\", CAST(QVIEW2.\"wlbDrillingOperator\" AS VARCHAR(10485760)) AS \"company\", \n" +
			"   4 AS \"yearQuestType\", NULL AS \"yearLang\", CAST(QVIEW2.\"wlbCompletionYear\" AS VARCHAR(10485760)) AS \"year\"\n" +
			" FROM \n" +
			"\"wellbore_development_all\" QVIEW1,\n" +
			"\"wellbore_exploration_all\" QVIEW2,\n" +
			"\"company\" QVIEW3,\n" +
			"\"wellbore_core\" QVIEW4,\n" +
			"\"wellbore_npdid_overview\" QVIEW5,\n" +
			"\"wellbore_core\" QVIEW6\n" +
			"WHERE \n" +
			"QVIEW1.\"wlbWellboreName\" IS NOT NULL AND\n" +
			"QVIEW1.\"wlbNpdidWellbore\" IS NOT NULL AND\n" +
			"(QVIEW1.\"wlbNpdidWellbore\" = QVIEW2.\"wlbNpdidWellbore\") AND\n" +
			"QVIEW2.\"wlbCompletionYear\" IS NOT NULL AND\n" +
			"(QVIEW2.\"wlbDrillingOperator\" = QVIEW3.\"cmpLongName\") AND\n" +
			"QVIEW2.\"wlbDrillingOperator\" IS NOT NULL AND\n" +
			"QVIEW3.\"cmpNpdidCompany\" IS NOT NULL AND\n" +
			"(QVIEW1.\"wlbNpdidWellbore\" = QVIEW4.\"wlbNpdidWellbore\") AND\n" +
			"(QVIEW1.\"wlbNpdidWellbore\" = QVIEW5.\"wlbNpdidWellbore\") AND\n" +
			"QVIEW4.\"wlbCoreNumber\" IS NOT NULL AND\n" +
			"(QVIEW4.\"wlbCoreNumber\" = QVIEW6.\"wlbCoreNumber\") AND\n" +
			"(QVIEW1.\"wlbNpdidWellbore\" = QVIEW6.\"wlbNpdidWellbore\") AND\n" +
			"QVIEW6.\"wlbTotalCoreLength\" IS NOT NULL AND\n" +
			"((QVIEW6.\"wlbTotalCoreLength\" > 50) AND (QVIEW2.\"wlbCompletionYear\" >= 2008))) SUB";
	
	public static final String queryExampleOrderBy="SELECT * from (SELECT \n" +
"   7 AS \"wellboreQuestType\", NULL AS \"wellboreLang\", CAST(QVIEW1.\"wlbWellboreName\" AS VARCHAR(10485760)) AS \"wellbore\", \n" +
"   5 AS \"lenghtMQuestType\", NULL AS \"lenghtMLang\", CAST(QVIEW6.\"wlbTotalCoreLength\" AS VARCHAR(10485760)) AS \"lenghtM\", \n" +
"   7 AS \"companyQuestType\", NULL AS \"companyLang\", CAST(QVIEW3.\"cmpLongName\" AS VARCHAR(10485760)) AS \"company\", \n" +
"   4 AS \"yearQuestType\", NULL AS \"yearLang\", CAST(QVIEW2.\"wlbCompletionYear\" AS VARCHAR(10485760)) AS \"year\"\n" +
" FROM \n" +
"\"wellbore_npdid_overview\" QVIEW1,\n" +
"\"wellbore_development_all\" QVIEW2,\n" +
"\"company\" QVIEW3,\n" +
"\"wellbore_exploration_all\" QVIEW4,\n" +
"\"wellbore_core\" QVIEW5,\n" +
"\"wellbore_core\" QVIEW6,\n" +
"\"wellbore_core\" QVIEW7\n" +
"WHERE \n" +
"QVIEW1.\"wlbNpdidWellbore\" IS NOT NULL AND\n" +
"QVIEW1.\"wlbWellboreName\" IS NOT NULL AND\n" +
"(QVIEW1.\"wlbNpdidWellbore\" = QVIEW2.\"wlbNpdidWellbore\") AND\n" +
"QVIEW2.\"wlbCompletionYear\" IS NOT NULL AND\n" +
"QVIEW3.\"cmpLongName\" IS NOT NULL AND\n" +
"QVIEW3.\"cmpNpdidCompany\" IS NOT NULL AND\n" +
"(QVIEW3.\"cmpLongName\" = QVIEW4.\"wlbDrillingOperator\") AND\n" +
"(QVIEW1.\"wlbNpdidWellbore\" = QVIEW4.\"wlbNpdidWellbore\") AND\n" +
"(QVIEW1.\"wlbNpdidWellbore\" = QVIEW5.\"wlbNpdidWellbore\") AND\n" +
"QVIEW5.\"wlbCoreNumber\" IS NOT NULL AND\n" +
"(QVIEW5.\"wlbCoreNumber\" = QVIEW6.\"wlbCoreNumber\") AND\n" +
"(QVIEW1.\"wlbNpdidWellbore\" = QVIEW6.\"wlbNpdidWellbore\") AND\n" +
"QVIEW6.\"wlbTotalCoreLength\" IS NOT NULL AND\n" +
"(QVIEW5.\"wlbCoreNumber\" = QVIEW7.\"wlbCoreNumber\") AND\n" +
"(QVIEW7.\"wlbCoreIntervalUom\" = '000001') AND\n" +
"(QVIEW1.\"wlbNpdidWellbore\" = QVIEW7.\"wlbNpdidWellbore\") AND\n" +
"((QVIEW6.\"wlbTotalCoreLength\" > 50) AND (QVIEW2.\"wlbCompletionYear\" >= 2008))\n" +
"UNION\n" +
"SELECT \n" +
"   7 AS \"wellboreQuestType\", NULL AS \"wellboreLang\", CAST(QVIEW1.\"wlbWellboreName\" AS VARCHAR(10485760)) AS \"wellbore\", \n" +
"   5 AS \"lenghtMQuestType\", NULL AS \"lenghtMLang\", CAST(QVIEW5.\"wlbTotalCoreLength\" AS VARCHAR(10485760)) AS \"lenghtM\", \n" +
"   7 AS \"companyQuestType\", NULL AS \"companyLang\", CAST(QVIEW2.\"wlbDrillingOperator\" AS VARCHAR(10485760)) AS \"company\", \n" +
"   4 AS \"yearQuestType\", NULL AS \"yearLang\", CAST(QVIEW2.\"wlbCompletionYear\" AS VARCHAR(10485760)) AS \"year\"\n" +
" FROM \n" +
"\"wellbore_npdid_overview\" QVIEW1,\n" +
"\"wellbore_development_all\" QVIEW2,\n" +
"\"company\" QVIEW3,\n" +
"\"wellbore_core\" QVIEW4,\n" +
"\"wellbore_core\" QVIEW5,\n" +
"\"wellbore_core\" QVIEW6\n" +
"WHERE \n" +
"QVIEW1.\"wlbNpdidWellbore\" IS NOT NULL AND\n" +
"QVIEW1.\"wlbWellboreName\" IS NOT NULL AND\n" +
"(QVIEW1.\"wlbNpdidWellbore\" = QVIEW2.\"wlbNpdidWellbore\") AND\n" +
"QVIEW2.\"wlbCompletionYear\" IS NOT NULL AND\n" +
"(QVIEW2.\"wlbDrillingOperator\" = QVIEW3.\"cmpLongName\") AND\n" +
"QVIEW2.\"wlbDrillingOperator\" IS NOT NULL AND\n" +
"QVIEW3.\"cmpNpdidCompany\" IS NOT NULL AND\n" +
"(QVIEW1.\"wlbNpdidWellbore\" = QVIEW4.\"wlbNpdidWellbore\") AND\n" +
"QVIEW4.\"wlbCoreNumber\" IS NOT NULL AND\n" +
"(QVIEW4.\"wlbCoreNumber\" = QVIEW5.\"wlbCoreNumber\") AND\n" +
"(QVIEW1.\"wlbNpdidWellbore\" = QVIEW5.\"wlbNpdidWellbore\") AND\n" +
"QVIEW5.\"wlbTotalCoreLength\" IS NOT NULL AND\n" +
"(QVIEW4.\"wlbCoreNumber\" = QVIEW6.\"wlbCoreNumber\") AND\n" +
"(QVIEW6.\"wlbCoreIntervalUom\" = '000001') AND\n" +
"(QVIEW1.\"wlbNpdidWellbore\" = QVIEW6.\"wlbNpdidWellbore\") AND\n" +
"((QVIEW5.\"wlbTotalCoreLength\" > 50) AND (QVIEW2.\"wlbCompletionYear\" >= 2008))\n" +
") SUB_QVIEW\n" +
"ORDER BY SUB_QVIEW.\"wellbore\"";
	
	public static void main(String[] args) throws Exception {
		
		String leftjoinsimple="select a.id from A a left join ( B b left join C c on b.id=c.id and b.n is not null)  on a.id=b.id";
		String file = readFile("/home/dimitris/example.sql");
		String simple="select A.id from A A, B B where A.id=B.id "
				+ "union "
				+ "select A.id from C C, A A, B B where B.name=C.name  and A.id=B.id  ";
		
		String testPlan="select m1.wellbore_mud_id from "
				+ "wellbore_mud m1, wellbore_mud m2, apaAreaGross g "
				+ "where "
				+ "m1.wellbore_mud_id=m2.wellbore_mud_id and "
				+ "m2.wellbore_mud_id=g.apaAreaGross_id";
				
		
		getDFLsFromDir("/home/dimitris/npdsql/existential/");
		/*NodeHashValues hashes=new NodeHashValues();
		NodeSelectivityEstimator nse = null;
		try {
			nse = new NodeSelectivityEstimator("/media/dimitris/T/exaremenpd100/" + "histograms.json");
		} catch (Exception e) {
			
		}
		hashes.setSelectivityEstimator(nse);
		SQLQuery query = SQLQueryParser.parse(testPlan, hashes);
		QueryDecomposer d = new QueryDecomposer(query, "/tmp/", 1, hashes);
		
		d.setN2a(new NamesToAliases());
		StringBuffer sb=new StringBuffer();
		for (SQLQuery s : d.getSubqueries()) {
			sb.append("\n");
			sb.append(s.toDistSQL());
		}
		System.out.println(sb.toString());*/
		

	}
	
	private static void getDFLsFromDir(String dir) throws Exception{
		for(String file:readFilesFromDir(dir)){
			getDFLFromFile(file);
		}
	}
	
	private static void getDFLFromFile(String file) throws Exception{
		String q = readFile(file);
		if(q.isEmpty()){
			return;
		}
		NodeHashValues hashes=new NodeHashValues();
		
		NodeSelectivityEstimator nse = null;
		try {
			nse = new NodeSelectivityEstimator("/media/dimitris/T/exaremenpd100/" + "histograms.json");
		} catch (Exception e) {
			
		}
		hashes.setSelectivityEstimator(nse);
		SQLQuery query = SQLQueryParser.parse(q, hashes);
		QueryDecomposer d = new QueryDecomposer(query, "/tmp/", 1, hashes);
		
		d.setN2a(new NamesToAliases());
		StringBuffer sb=new StringBuffer();
		for (SQLQuery s : d.getSubqueries()) {
			sb.append("\n");
			sb.append(s.toDistSQL());
		}
		writeFile(file+".dfl", sb.toString());
	}
	
	private static String readFile(String file) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(file));
		String line = null;
		StringBuilder stringBuilder = new StringBuilder();
		String ls = System.getProperty("line.separator");

		while ((line = reader.readLine()) != null) {
			stringBuilder.append(line);
			stringBuilder.append(ls);
		}
		reader.close();
		return stringBuilder.toString();
	}
	
    private static String[] readFilesFromDir(String string) throws IOException {
    	File folder = new File(string);
    	File[] listOfFiles = folder.listFiles();
    	List<String> files=new ArrayList<String>();
    	    for (int i = 0; i < listOfFiles.length; i++) {
    	      if (listOfFiles[i].isFile()&&listOfFiles[i].getCanonicalPath().endsWith("10.q.sql")) {
    	    	  files.add(listOfFiles[i].getCanonicalPath());
    	      }
    	    }
    	    return files.toArray(new String[files.size()]);
	}

	public static void writeFile(String filename, String string) {
		writeFile(filename, string.getBytes());
	}
	public static void writeFile(String filename, byte[] string) {
		try {
			File file = new File(filename);
			file.getParentFile().mkdirs();
			OutputStream out = new FileOutputStream(file);
			out.write(string);
			out.close();
		} catch (Exception e) {
			System.err.println("Error writing file: " + filename);
			e.printStackTrace();
		}
	}
    
}

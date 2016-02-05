package madgik.exareme.master.queryProcessor.decomposer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;

import madgik.exareme.master.queryProcessor.decomposer.dag.NodeHashValues;
import madgik.exareme.master.queryProcessor.decomposer.federation.DBInfoReaderDB;
import madgik.exareme.master.queryProcessor.decomposer.federation.NamesToAliases;
import madgik.exareme.master.queryProcessor.decomposer.federation.QueryDecomposer;
import madgik.exareme.master.queryProcessor.decomposer.query.SQLQuery;
import madgik.exareme.master.queryProcessor.decomposer.query.SQLQueryParser;

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
	
	public static void main(String[] args) throws Exception {
		String leftjoinsimple="select a.id from A a left join ( B b left join C c on b.id=c.id and b.n is not null)  on a.id=b.id";
		
		String file = readFile("/home/dimitris/example.sql");
		NodeHashValues hashes=new NodeHashValues();
		hashes.setSelectivityEstimator(null);
		SQLQuery query = SQLQueryParser.parse(leftjoinsimple, hashes);
		QueryDecomposer d = new QueryDecomposer(query, "/tmp/", 2, hashes);
		
		d.setN2a(new NamesToAliases());
		
		for (SQLQuery s : d.getSubqueries()) {
			System.out.println(s.getHashId()+" : \n"+s.toDistSQL());
		}
		

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

}

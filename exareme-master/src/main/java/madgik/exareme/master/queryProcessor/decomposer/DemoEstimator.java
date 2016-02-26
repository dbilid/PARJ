package madgik.exareme.master.queryProcessor.decomposer;

import madgik.exareme.master.queryProcessor.decomposer.dag.NodeHashValues;
import madgik.exareme.master.queryProcessor.decomposer.federation.NamesToAliases;
import madgik.exareme.master.queryProcessor.decomposer.federation.QueryDecomposer;
import madgik.exareme.master.queryProcessor.decomposer.query.SQLQuery;
import madgik.exareme.master.queryProcessor.decomposer.query.SQLQueryParser;
import madgik.exareme.master.queryProcessor.estimator.NodeSelectivityEstimator;

public class DemoEstimator {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		NodeHashValues hashes=new NodeHashValues();
		NodeSelectivityEstimator nse=null;
		String core="select qview5.wlbCoreNumber from wellbore_core qview5, wellbore_core qview6 where (qview5.wlbCoreNumber = qview6.wlbCoreNumber)";

		String q2="select qview5.wlbNpdidWellbore from wellbore_core qview5, wellbore_exploration_all qview4 where qview5.wlbNpdidWellbore=qview4.wlbNpdidWellbore";
		
		String q="select m.wlbMD as md from wellbore_mud m, apaAreaGross g where m.wlbMD=g.apaAreaGross_id";
		nse = new NodeSelectivityEstimator("/media/dimitris/T/exaremenpd100/" + "histograms.json");
		hashes.setSelectivityEstimator(nse);
		SQLQuery query = SQLQueryParser.parse(q2, hashes);
		QueryDecomposer d = new QueryDecomposer(query, "/tmp/", 1, hashes);
		
		d.setN2a(new NamesToAliases());
		StringBuffer sb=new StringBuffer();
		for (SQLQuery s : d.getSubqueries()) {
			sb.append("\n");
			sb.append(s.toDistSQL());
		}
	}

}

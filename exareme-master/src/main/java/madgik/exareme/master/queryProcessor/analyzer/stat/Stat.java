/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package madgik.exareme.master.queryProcessor.analyzer.stat;

import madgik.exareme.master.queryProcessor.decomposer.DecomposerUtils;
import madgik.exareme.master.queryProcessor.decomposer.query.Column;
import madgik.exareme.master.queryProcessor.estimator.db.AttrInfo;
import madgik.exareme.master.queryProcessor.estimator.db.RelInfo;
import madgik.exareme.master.queryProcessor.estimator.db.Schema;
import madgik.exareme.master.queryProcessor.estimator.histogram.Bucket;
import madgik.exareme.master.queryProcessor.estimator.histogram.Histogram;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * @author jim
 */
public class Stat {
	
	private static final Logger log = Logger.getLogger(Stat.class);

	private final Connection con;

	public Stat(Connection con) {
		this.con = con;
	}

	// schema map
	//private Map<Integer, Table> schema = new HashMap<Integer, Table>();
	
	

	public Schema extractSPARQLStats() throws Exception {
		Map<Integer, RelInfo> relMap = new HashMap<Integer, RelInfo>();
		Schema schema = new Schema("FULL_SCHEMA", relMap);
		Statement st = con.createStatement();
		ResultSet resultTables = st.executeQuery("select id, uri from properties");
		log.debug("Starting extracting stats");
		int typeProperty=-1;
		while (resultTables.next()) {
			if(resultTables.getString(2).equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")){
				typeProperty=resultTables.getInt(1);
				//continue;
			}
			//if(resultTables.getInt(1)>-1) continue;
			Map<Column, AttrInfo> attrIndex = new HashMap<Column, AttrInfo>();
			String tableName = "prop" + resultTables.getInt(1) ;
			log.debug("Analyzing table " + tableName);

			

			int count = getCount(tableName);

			if (count == 0) {
				log.debug("Empty table");
				continue;
			}
			String columnName = "s";
			Column col=new Column(resultTables.getInt(1), true);
			String inv = "";
			for (int h = 0; h < 2; h++) {

				try {

					// computing column's min and max values
					MinMax mm = computeMinMax(inv + "prop" + resultTables.getInt(1), columnName);
					double minVal = mm.getMin();
					double maxVal = mm.getMax();

					
					int diffVals = getCount("(select distinct " + columnName + " from " + inv + "prop" + resultTables.getInt(1) + ")");
					
					// for (ValFreq k : freqs) {
					// diffValFreqMap.put(k.getVal(), k.getFreq());

					// }
					int freq = count/diffVals;

					
					//StatColumn c = new StatColumn(col, Types.INTEGER, 4, diffVals, minVal, maxVal);
					
					NavigableMap<Double, Bucket> bucketIndex = new TreeMap<Double, Bucket>();
					log.debug("building primitive histogram for column:" + col);

					
				
					Bucket b = new Bucket((double) freq, (double) diffVals);

					bucketIndex.put(minVal, b);
					bucketIndex.put(Math.nextAfter(maxVal,
							Double.MAX_VALUE), Bucket.FINAL_HISTOGRAM_BUCKET);
					Histogram hist = new Histogram(bucketIndex);
					if(typeProperty!=resultTables.getInt(1)){
					PreparedStatement ps=con.prepareStatement("select "+columnName+" from "+inv +tableName+" order by "+
					columnName+"  limit 1 offset?1");
					splitBuckets(hist, minVal, 0, 0, ps);
					ps.close();
					}
					
					AttrInfo a = new AttrInfo(col, hist, 4);
					attrIndex.put(col, a);
					
					//columnMap.put(col, c);
				} catch (Exception ex) {
					log.error("could not analyze column " + columnName + ":" + ex.getMessage());
				}
				columnName = "o";
				col=new Column(resultTables.getInt(1), false);
				inv = "inv";
			}
			RelInfo r = new RelInfo(resultTables.getInt(1), attrIndex, count, 8);

			relMap.put(r.getRelName(), r);
			//Table t = new Table("prop" + resultTables.getInt(1), columnCount, tupleSize, columnMap, count);
			//schema.put(resultTables.getInt(1), t);

		}
		resultTables.close();
		if(typeProperty>-1){
			gatherTypeStats(typeProperty, st, relMap);
		}
		
		st.close();
		return schema;

	}

	private void gatherTypeStats(int typePropNo, Statement st, Map<Integer, RelInfo> relMap) {
		
		log.debug("Analyzing type information");
		
		
		try {
			
		ResultSet types=st.executeQuery("select distinct o from invprop"+typePropNo);
		
		while(types.next()){
			Map<Column, AttrInfo> attrIndex = new HashMap<Column, AttrInfo>();
			int no=types.getInt(1);
		String tableName="invprop"+typePropNo+" where o="+no;
		int count = getCount(tableName);

		if (count == 0) {
			log.debug("Empty table");
			continue;
		}
		String columnName = "s";
		Column col=new Column(-no, true);
				// computing column's min and max values
				MinMax mm = computeMinMax(tableName, columnName);
				double minVal = mm.getMin();
				double maxVal = mm.getMax();

				
				NavigableMap<Double, Bucket> bucketIndex = new TreeMap<Double, Bucket>();
				log.debug("building primitive histogram for column:" + col);

				
			
				Bucket b = new Bucket(1.0, (double)count);

				bucketIndex.put(minVal, b);
				bucketIndex.put(Math.nextAfter(maxVal,
						Double.MAX_VALUE), Bucket.FINAL_HISTOGRAM_BUCKET);
				Histogram hist = new Histogram(bucketIndex);
				PreparedStatement ps=con.prepareStatement("select "+columnName+" from "+tableName+" order by "+
						columnName+"  limit 1 offset ?1");
						splitBuckets(hist, minVal, 0, 0, ps);
						ps.close();
				AttrInfo a = new AttrInfo(col, hist, 4);
				attrIndex.put(col, a);
				

				NavigableMap<Double, Bucket> bucketIndex2 = new TreeMap<Double, Bucket>();
					
				columnName = "second";
				col=new Column(-no, false);
				minVal = (double)no;
				maxVal = minVal;
				Bucket b2 = new Bucket((double) count, 1.0);

				bucketIndex2.put(minVal, b2);
				bucketIndex2.put(Math.nextAfter(maxVal,
						Double.MAX_VALUE), Bucket.FINAL_HISTOGRAM_BUCKET);
				Histogram hist2 = new Histogram(bucketIndex2);

				AttrInfo a2 = new AttrInfo(col, hist2, 4);
				attrIndex.put(col, a2);

				
			
			
				RelInfo r = new RelInfo(-no, attrIndex, count, 8);

				relMap.put(r.getRelName(), r);

		}
		} catch (Exception ex) {
			log.error("could not analyze type table:" + ex.getMessage());
		}
		
	}

	

	private void splitBuckets(Histogram hist, double minVal, int recursion, int start, PreparedStatement ps) throws SQLException {
		System.out.println("splitting, rec:"+recursion+" minVal:" +minVal+"start: "+start);
		if(recursion>DecomposerUtils.SPLIT_BUCKET_THRESHOLD){
			return;
		}
		double maxVal=hist.getBucketIndex().higherKey(minVal);
		double count=hist.getBucketIndex().get(minVal).getDiffValues()*hist.getBucketIndex().get(minVal).getFrequency();
		double range=maxVal-minVal;
		ps.setInt(1, start+(int)(count/2));
		ResultSet rs=ps.executeQuery();
		int median=0;
		if(rs.next()){
			median=rs.getInt(1);
		}
		else{
			System.err.println("median not returned");
			return;
		}
		rs.close();
		//double idealMedian=minVal+range/2;
		//if(Math.abs(median-idealMedian)>(range/10)){
			hist.splitBucket(minVal, median);
			splitBuckets(hist, minVal, recursion+1, start, ps);
			splitBuckets(hist, median, recursion+1, start+(int)(count/2), ps);
		//}
		
		
	}

	private MinMax computeMinMax(String tableName, String columnName) throws Exception {
		String query1 = "select min(`" + columnName + "`) as minVal, max(`" + columnName + "`) " + "as maxVal  from "
				+ tableName ;

		double minVal = 0, maxVal=0;

		Statement stmt1 = con.createStatement();
		ResultSet rs1 = stmt1.executeQuery(query1);
		while (rs1.next()) {
			minVal = (double)rs1.getInt("minVal");
			maxVal = (double)rs1.getInt("maxVal");
		}
		rs1.close();
		stmt1.close();

		return new MinMax(minVal, maxVal);
	}

	

	private int getCount(String tableName) throws SQLException {
		String query1 = "select count(*) from " + tableName;
		Statement stmt1 = con.createStatement();
		ResultSet rs1 = stmt1.executeQuery(query1);
		int result = 0;
		while (rs1.next()) {
			result = rs1.getInt(1);
		}
		rs1.close();
		stmt1.close();

		return result;
	}


	

	/* inner - helper classes */
	private final class MinMax {
		private final double min;
		private final double max;

		public MinMax(double min, double max) {
			this.min = min;
			this.max = max;
		}

		public double getMin() {
			return min;
		}

		public double getMax() {
			return max;
		}

		@Override
		public String toString() {
			return "MinMax{" + "min=" + min + ", max=" + max + '}';
		}

	}


}

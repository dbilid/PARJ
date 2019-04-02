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
import org.apache.log4j.Logger;
import java.sql.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author jim
 */
public class MemoryStat {

	private static final Logger log = Logger.getLogger(MemoryStat.class);

	private final Connection con;
	private List<TableSize> sizes;
	private int properties;

	public MemoryStat(Connection con, int properties) {
		this.con = con;
		sizes = new ArrayList<TableSize>();
		this.properties = properties;
	}

	// schema map
	// private Map<Integer, Table> schema = new HashMap<Integer, Table>();

	public Schema extractSPARQLStats() throws Exception {
		Map<Integer, RelInfo> relMap = new HashMap<Integer, RelInfo>();
		Schema schema = new Schema("FULL_SCHEMA", relMap);

		Statement tbls = con.createStatement();
		ResultSet resultTables = tbls.executeQuery("select id, uri from properties");
		log.debug("Starting extracting stats");
		int typeProperty = -1;

		while (resultTables.next()) {
			if (resultTables.getString(2).equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
				typeProperty = resultTables.getInt(1);
				break;
			}
		}
		Statement st = con.createStatement();
		st.execute("create virtual table stat2 using stat(" + properties + ", " + typeProperty + ")");
		st.close();
		resultTables.close();
		tbls.close();
		Statement st2 = con.createStatement();
		ResultSet mode0 = st2.executeQuery("select result from stat2 where mode=0");
		while (mode0.next()) {
			// if(resultTables.getInt(1)>-1) continue;
			Map<Column, AttrInfo> attrIndex = new HashMap<Column, AttrInfo>();
			int propno = mode0.getInt(1);
			log.debug("Analyzing table " + propno);
			mode0.next();
			int inverse = mode0.getInt(1);
			mode0.next();
			double minVal = mode0.getDouble(1);
			mode0.next();
			double maxVal = mode0.getDouble(1);
			mode0.next();
			int diffVals = mode0.getInt(1);
			mode0.next();
			int count = mode0.getInt(1);

			if (count == 0) {
				log.debug("Empty table");
				continue;
			}
			Column col = null;
			if (inverse == 0) {
				col = new Column(propno, true);
			} else {
				col = new Column(propno, false);
			}

			int freq = count / diffVals;

			NavigableMap<Double, Bucket> bucketIndex = new TreeMap<Double, Bucket>();
			log.debug("building primitive histogram for column:" + col);

			Bucket b = new Bucket((double) freq, (double) diffVals);

			bucketIndex.put(minVal, b);
			bucketIndex.put(Math.nextAfter(maxVal, Double.MAX_VALUE), Bucket.FINAL_HISTOGRAM_BUCKET);
			Histogram hist = new Histogram(bucketIndex);

			AttrInfo a = new AttrInfo(col, hist, 4);
			attrIndex.put(col, a);

			mode0.next();
			propno = mode0.getInt(1);
			mode0.next();
			inverse = mode0.getInt(1);
			mode0.next();
			minVal = mode0.getDouble(1);
			mode0.next();
			maxVal = mode0.getDouble(1);
			mode0.next();
			diffVals = mode0.getInt(1);
			mode0.next();
			count = mode0.getInt(1);

			if (count == 0) {
				log.debug("Empty table");
				continue;
			}
			Column col2 = null;
			if (inverse == 0) {
				col2 = new Column(propno, true);
			} else {
				col2 = new Column(propno, false);
			}

			freq = count / diffVals;

			NavigableMap<Double, Bucket> bucketIndex2 = new TreeMap<Double, Bucket>();
			log.debug("building primitive histogram for column:" + col);

			Bucket b2 = new Bucket((double) freq, (double) diffVals);

			bucketIndex2.put(minVal, b2);
			bucketIndex2.put(Math.nextAfter(maxVal, Double.MAX_VALUE), Bucket.FINAL_HISTOGRAM_BUCKET);
			Histogram hist2 = new Histogram(bucketIndex2);

			AttrInfo a2 = new AttrInfo(col2, hist2, 4);
			attrIndex.put(col2, a2);

			RelInfo r = new RelInfo(propno, attrIndex, count, 8);

			relMap.put(r.getRelName(), r);
			sizes.add(new TableSize(r.getNumberOfTuples(), propno));
			// Table t = new Table("prop" + resultTables.getInt(1), columnCount, tupleSize,
			// columnMap, count);
			// schema.put(resultTables.getInt(1), t);

		}
		mode0.close();
		st2.close();
		Statement st3 = con.createStatement();
		if (typeProperty > -1) {
			gatherTypeStats(typeProperty, st3, relMap);
		}
		st3.close();
		// typeProperty=-1;
		schema.setCards(computeJoins(typeProperty));

		return schema;

	}

	private JoinCardinalities computeJoins(int typeProperty) throws Exception {
		JoinCardinalities cards = new JoinCardinalities();
		sizes.sort(new SizeComparator());
		// Statement stmt1 = con.createStatement();
		ExecutorService exService = Executors.newFixedThreadPool(DecomposerUtils.CARDINALITY_THREADS);
		try {

			ExecutorCompletionService<Boolean> ecs = new ExecutorCompletionService<Boolean>(exService);
			for (int i = 0; i < sizes.size(); i++) {
				ecs.submit(new CardinalityEstimator(i, typeProperty, cards));

			}
			for (int i = 0; i < sizes.size(); i++) {
				Future<Boolean> result = ecs.take();
				if (!result.get()) {
					System.err.println("Could not compute join cardinalities");
				}
			}

		} finally {
			exService.shutdown();
			exService.awaitTermination(3600, TimeUnit.SECONDS);
		}

		/*
		 * int tblName = 0; TableSize ts = sizes.get(i); if (ts.getTable() ==
		 * typeProperty) { continue; } tblName = ts.getTable();
		 * 
		 * for (int j = i + 1; j < sizes.size(); j++) { int tblName2 = 0; TableSize ts2
		 * = sizes.get(j); if (ts2.getTable() == typeProperty) { continue; } //
		 * if(ts.getTable()<0 && ts2.getTable()<0){ // continue; // } tblName2 =
		 * ts2.getTable();
		 * 
		 * ResultSet mode2 = stmt1.
		 * executeQuery("select result from stat2 where mode=2 and option1=0 and option2="
		 * + tblName + " and option3=" + tblName2);
		 * 
		 * int countSS = mode2.getInt(1); int countSO = 0; int countOS = 0; int countOO
		 * = 0; mode2.close(); if (ts2.getTable() > -1) { mode2 = stmt1.
		 * executeQuery("select result from stat2 where mode=2 and option1=1 and option2="
		 * + tblName + " and option3=" + tblName2); countSO = mode2.getInt(1);
		 * mode2.close(); } if (ts.getTable() != typeProperty) { mode2 = stmt1.
		 * executeQuery("select result from stat2 where mode=2 and option1=2 and option2="
		 * + tblName + " and option3=" + tblName2); countOS = mode2.getInt(1);
		 * mode2.close(); if (ts2.getTable() > -1) { mode2 = stmt1.
		 * executeQuery("select result from stat2 where mode=2 and option1=3 and option2="
		 * + tblName + " and option3=" + tblName2); countOO = mode2.getInt(1);
		 * mode2.close();
		 * 
		 * } }
		 * 
		 * if (ts.getTable() < ts2.getTable()) { cards.add(ts.getTable(),
		 * ts2.getTable(), countSS, countSO, countOS, countOO); } else {
		 * cards.add(ts2.getTable(), ts.getTable(), countSS, countOS, countSO, countOO);
		 * }
		 * 
		 * }
		 */
		// }
		return cards;
	}

	private void gatherTypeStats(int typePropNo, Statement st, Map<Integer, RelInfo> relMap) {

		log.debug("Analyzing type information");

		try {
			ResultSet mode1 = st.executeQuery("select result from stat2 where mode=1 and option1=" + typePropNo);
			// ResultSet types = st.executeQuery("select distinct o from invprop" +
			// typePropNo);

			while (mode1.next()) {
				Map<Column, AttrInfo> attrIndex = new HashMap<Column, AttrInfo>();
				int no = mode1.getInt(1);
				mode1.next();
				double minVal = mode1.getDouble(1);
				mode1.next();
				double maxVal = mode1.getDouble(1);
				mode1.next();
				int count = mode1.getInt(1);

				if (count == 0) {
					log.debug("Empty table");
					continue;
				}
				Column col = new Column(-no, true);

				NavigableMap<Double, Bucket> bucketIndex = new TreeMap<Double, Bucket>();
				log.debug("building primitive histogram for column:" + col);

				Bucket b = new Bucket(1.0, (double) count);

				bucketIndex.put(minVal, b);
				bucketIndex.put(Math.nextAfter(maxVal, Double.MAX_VALUE), Bucket.FINAL_HISTOGRAM_BUCKET);
				Histogram hist = new Histogram(bucketIndex);

				AttrInfo a = new AttrInfo(col, hist, 4);
				attrIndex.put(col, a);

				NavigableMap<Double, Bucket> bucketIndex2 = new TreeMap<Double, Bucket>();

				col = new Column(-no, false);
				minVal = (double) no;
				maxVal = minVal;
				Bucket b2 = new Bucket((double) count, 1.0);

				bucketIndex2.put(minVal, b2);
				bucketIndex2.put(Math.nextAfter(maxVal, Double.MAX_VALUE), Bucket.FINAL_HISTOGRAM_BUCKET);
				Histogram hist2 = new Histogram(bucketIndex2);

				AttrInfo a2 = new AttrInfo(col, hist2, 4);
				attrIndex.put(col, a2);

				RelInfo r = new RelInfo(-no, attrIndex, count, 8);

				relMap.put(r.getRelName(), r);
				sizes.add(new TableSize(r.getNumberOfTuples(), -no));

			}
			mode1.close();
		} catch (Exception ex) {
			log.error("could not analyze type table:" + ex.getMessage());
		}

	}

	private class SizeComparator implements Comparator<TableSize> {
		@Override
		public int compare(TableSize a, TableSize b) {
			return a.getSize().compareTo(b.getSize());
		}
	}

	private class CardinalityEstimator implements Callable<Boolean> {

		int propIndex;
		int typeProperty;
		JoinCardinalities cards;

		private CardinalityEstimator(int prop, int type, JoinCardinalities cardinalities) {
			this.propIndex = prop;
			this.typeProperty = type;
			this.cards = cardinalities;
		}

		@Override
		public Boolean call() {
			int tblName = 0;
			TableSize ts = sizes.get(propIndex);
			if (ts.getTable() == typeProperty) {
				return true;
			}
			JoinCardinalities temp = new JoinCardinalities();
			tblName = ts.getTable();
			Statement stmt1;
			try {
				stmt1 = con.createStatement();

				for (int j = propIndex + 1; j < sizes.size(); j++) {
					int tblName2 = 0;
					TableSize ts2 = sizes.get(j);
					if (ts2.getTable() == typeProperty) {
						continue;
					}
					// if(ts.getTable()<0 && ts2.getTable()<0){
					// continue;
					// }
					tblName2 = ts2.getTable();

					ResultSet mode2 = stmt1
							.executeQuery("select result from stat2 where mode=2 and option1=0 and option2=" + tblName
									+ " and option3=" + tblName2);
					/*
					 * String querySS = "(select * from " + tblName + " a cross join " + tblName2 +
					 * " b where a.s=b.s limit 300000000)"; String querySO = "(select * from " +
					 * tblName + " a cross join " + inv2 + tblName2 +
					 * " b where a.s=b.o limit 300000000)"; String queryOS = "(select * from " +
					 * inv1 + tblName + " a cross join " + tblName2 +
					 * " b where a.o=b.s limit 300000000)"; String queryOO = "(select * from " +
					 * inv1 + tblName + " a cross join " + inv2 + tblName2 +
					 * " b where a.o=b.o limit 300000000)";
					 */
					int countSS = mode2.getInt(1);
					int countSO = 0;
					int countOS = 0;
					int countOO = 0;
					mode2.close();
					if (ts2.getTable() > -1) {
						mode2 = stmt1.executeQuery("select result from stat2 where mode=2 and option1=1 and option2="
								+ tblName + " and option3=" + tblName2);
						countSO = mode2.getInt(1);
						mode2.close();
					}
					if (ts.getTable() != typeProperty) {
						mode2 = stmt1.executeQuery("select result from stat2 where mode=2 and option1=2 and option2="
								+ tblName + " and option3=" + tblName2);
						countOS = mode2.getInt(1);
						mode2.close();
						if (ts2.getTable() > -1) {
							mode2 = stmt1
									.executeQuery("select result from stat2 where mode=2 and option1=3 and option2="
											+ tblName + " and option3=" + tblName2);
							countOO = mode2.getInt(1);
							mode2.close();

						}
					}

					if (ts.getTable() < ts2.getTable()) {
						temp.add(ts.getTable(), ts2.getTable(), countSS, countSO, countOS, countOO);
					} else {
						temp.add(ts2.getTable(), ts.getTable(), countSS, countOS, countSO, countOO);
					}

				}
				stmt1.close();
				synchronized (this) {
					cards.addAll(temp);
				}
				return true;
			} catch (SQLException e) {
				System.err.println("Could not estimate cardinality");
				e.printStackTrace();
				return false;
			}

		}
	}

}

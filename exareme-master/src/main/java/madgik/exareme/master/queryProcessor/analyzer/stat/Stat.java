/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package madgik.exareme.master.queryProcessor.analyzer.stat;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * @author jim
 */
public class Stat implements StatExtractor {
	// public static final int LIMIT = 10000;
	// public static final double LIMIT_FACTOR = 0.3;
	private static final int BLOB_SIZE = 1000000;
	private static final int NUM_SIZE = 8;
	private static final int MAX_STRING_SAMPLE = 20;
	// public static final String SAMPLE = "_sample";
	private static final Logger log = Logger.getLogger(Stat.class);

	private final Connection con;
	private String sch;

	public Stat(Connection con) {
		sch = "";
		this.con = con;
	}

	// schema map
	private Map<String, Table> schema = new HashMap<String, Table>();

	@Override
	public Map<String, Table> extractStats() throws Exception {

		DatabaseMetaData dbmd = con.getMetaData(); // dtabase metadata object

		// listing tables and columns
		String catalog = null;
		String schemaPattern = null;
		String tableNamePattern = null;
		String[] types = null;
		String columnNamePattern = null;

		ResultSet resultTables = dbmd.getTables(catalog, schemaPattern, tableNamePattern, types);
		log.debug("Starting extracting stats");
		while (resultTables.next()) {
			Map<String, Column> columnMap = new HashMap<String, Column>();
			String tableName = StringEscapeUtils.escapeJava(resultTables.getString(3));
			log.debug("Analyzing table " + tableName);

			int columnCount = resultTables.getMetaData().getColumnCount();
			int tupleSize = 0; // in bytes

			tableNamePattern = tableName;
			ResultSet resultColumns = dbmd.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);

			int count = getCount(tableName);

			if (count == 0) {
				log.debug("Empty table");
				continue;
			}

			while (resultColumns.next()) {

				String columnName = StringEscapeUtils.escapeJava(resultColumns.getString(4));
				try {
					int columnType = resultColumns.getInt(5);

					// computing column's size in bytes
					int columnSize = computeColumnSize(columnName, columnType, tableName);
					tupleSize += columnSize;

					// execute queries for numberOfDiffValues, minVal, maxVal
					// Map<String, Integer> diffValFreqMap = new HashMap<String,
					// Integer>();

					// computing column's min and max values
					MinMax mm = computeMinMax(tableName, columnName);
					String minVal = mm.getMin();
					String maxVal = mm.getMax();

					// /
					Map<String, Integer> diffValFreqMap = computeDistinctValuesFrequency(tableName, columnName);

					// for (ValFreq k : freqs) {
					// diffValFreqMap.put(k.getVal(), k.getFreq());

					// }

					// /add min max diff vals in the sampling values
					int minOcc = computeValOccurences(tableName, columnName, minVal);
					if (!diffValFreqMap.containsKey(minVal))
						diffValFreqMap.put(minVal, minOcc);
					int maxOcc = computeValOccurences(tableName, columnName, maxVal);
					if (!diffValFreqMap.containsKey(maxVal))
						diffValFreqMap.put(maxVal, maxOcc);

					int diffVals = diffValFreqMap.size();

					Column c = new Column(columnName, columnType, columnSize, diffVals, minVal, maxVal, diffValFreqMap);
					columnMap.put(columnName, c);
				} catch (Exception ex) {
					log.error("could not analyze column " + columnName + ":" + ex.getMessage());
				}

			}
			resultColumns.close();
			ResultSet pkrs = dbmd.getExportedKeys("", "", tableName);
			String pkey = "DEFAULT_KEY";

			while (pkrs.next()) {
				pkey = pkrs.getString("PKCOLUMN_NAME");
				break;
			}
			pkrs.close();
			Table t = new Table(tableName, columnCount, tupleSize, columnMap, count, pkey);
			schema.put(tableName, t);

		}
		resultTables.close();
		return schema;

	}

	public Map<String, Table> extractSPARQLStats(int partitions) throws Exception {

		Statement st = con.createStatement();
		ResultSet resultTables = st.executeQuery("select id from properties");
		log.debug("Starting extracting stats");
		while (resultTables.next()) {
			Map<String, Column> columnMap = new HashMap<String, Column>();
			String tableName = "prop" + resultTables.getInt(1) + "_0";
			log.debug("Analyzing table " + tableName);

			int columnCount = 2;
			int tupleSize = 8; // in bytes

			int count = getCount(tableName);

			if (count == 0) {
				log.debug("Empty table");
				continue;
			}
			String columnName = "s";
			String inv = "";
			for (int h = 0; h < 2; h++) {

				try {

					// computing column's min and max values
					MinMax mm = computeMinMaxPartitioned(inv + "prop" + resultTables.getInt(1) + "_", columnName,
							partitions);
					String minVal = mm.getMin();
					String maxVal = mm.getMax();

					// /
					Map<String, Integer> diffValFreqMap = computeDistinctValuesFrequencySPARQL(
							inv + "prop" + resultTables.getInt(1) + "_0", columnName, partitions);

					if (diffValFreqMap.isEmpty()) {
						// empty partition 0
						diffValFreqMap.put(minVal, count);
					}

					// for (ValFreq k : freqs) {
					// diffValFreqMap.put(k.getVal(), k.getFreq());

					// }
					int freq = diffValFreqMap.values().iterator().next();

					// /add min max diff vals in the sampling values
					// int minOcc = computeValOccurences(tableName, columnName,
					// minVal);
					if (!diffValFreqMap.containsKey(minVal))
						diffValFreqMap.put(minVal, freq);
					// int maxOcc = computeValOccurences(tableName, columnName,
					// maxVal);
					if (!diffValFreqMap.containsKey(maxVal))
						diffValFreqMap.put(maxVal, freq);

					int diffVals = (count * partitions / freq);

					Column c = new Column(columnName, Types.INTEGER, 4, diffVals, minVal, maxVal, diffValFreqMap);
					columnMap.put(columnName, c);
				} catch (Exception ex) {
					log.error("could not analyze column " + columnName + ":" + ex.getMessage());
				}
				columnName = "o";
				inv = "inv";
			}
			String pkey = "DEFAULT_KEY";

			Table t = new Table("prop" + resultTables.getInt(1), columnCount, tupleSize, columnMap, count * partitions,
					pkey);
			schema.put("prop" + resultTables.getInt(1), t);

		}
		resultTables.close();
		return schema;

	}

	/* private-helper methods */
	private int computeColumnSize(String columnName, int columnType, String table_sample) throws Exception {
		int columnSize = 0;
		if (columnType == Types.INTEGER || columnType == Types.REAL || columnType == Types.DOUBLE
				|| columnType == Types.DECIMAL || columnType == Types.FLOAT || columnType == Types.NUMERIC) {
			columnSize = NUM_SIZE;
		} else if (columnType == Types.VARCHAR) {
			String query0 = "select max(length(`" + columnName + "`)) as length from (select `" + columnName
					+ "` from `" + table_sample + "`)" + " where `" + columnName + "` is not null limit "
					+ MAX_STRING_SAMPLE;

			Statement stmt0 = con.createStatement();
			ResultSet rs0 = stmt0.executeQuery(query0);

			while (rs0.next()) {
				columnSize = rs0.getInt("length");
			}
			rs0.close();
			stmt0.close();

		} else if (columnType == Types.BLOB)
			columnSize = BLOB_SIZE;

		return columnSize;
	}

	private MinMax computeMinMax(String tableName, String columnName) throws Exception {
		String query1 = "select min(`" + columnName + "`) as minVal, max(`" + columnName + "`) " + "as maxVal  from `"
				+ tableName + "` where `" + columnName + "` is not null";

		String minVal = "", maxVal = "";

		Statement stmt1 = con.createStatement();
		ResultSet rs1 = stmt1.executeQuery(query1);
		while (rs1.next()) {
			minVal = rs1.getString("minVal");
			maxVal = rs1.getString("maxVal");
		}
		rs1.close();
		stmt1.close();

		return new MinMax(minVal, maxVal);
	}

	private MinMax computeMinMaxPartitioned(String tableName, String columnName, int partitions) throws Exception {
		StringBuffer query1 = new StringBuffer();
		query1.append("select min(");
		query1.append(columnName);
		query1.append(") as minVal, max(");
		query1.append(columnName);
		query1.append(") as maxVal  from (");
		String union = "";
		for (int i = 0; i < partitions; i++) {
			query1.append(union);
			query1.append(" select * from ");
			query1.append(tableName);
			query1.append(i);
			union = " union all ";
		}
		query1.append(")");
		String minVal = "", maxVal = "";

		Statement stmt1 = con.createStatement();
		ResultSet rs1 = stmt1.executeQuery(query1.toString());
		while (rs1.next()) {
			minVal = rs1.getString("minVal");
			maxVal = rs1.getString("maxVal");
		}
		rs1.close();
		stmt1.close();

		return new MinMax(minVal, maxVal);
	}

	private int computeValOccurences(String tableName, String columnName, String value) throws Exception {
		String queryDf = "select count(*) as valCount " + "from `" + tableName + "` where `" + columnName
				+ "` is not null and  `" + columnName + "` = \"" + value + "\"";
		Statement stmt = con.createStatement();
		ResultSet rs = stmt.executeQuery(queryDf);
		int diffValCount = 0;
		while (rs.next()) {
			diffValCount = rs.getInt("valCount");
		}
		rs.close();
		stmt.close();

		return diffValCount;
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

	private Map<String, Integer> computeDistinctValuesFrequency(String table_sample, String columnName)
			throws Exception {
		Map<String, Integer> freqs = new HashMap<String, Integer>();

		String query = "select `" + columnName + "` as val, count(*) as freq from `" + table_sample + "` where `"
				+ columnName + "` is not null group by `" + columnName + "`";

		Statement stmt = con.createStatement();
		ResultSet rs = stmt.executeQuery(query);

		while (rs.next()) {
			freqs.put(rs.getString("val"), rs.getInt("freq"));
		}

		rs.close();
		stmt.close();

		return freqs;
	}

	private Map<String, Integer> computeDistinctValuesFrequencySPARQL(String table_sample, String columnName,
			int partitions) throws Exception {
		Map<String, Integer> freqs = new HashMap<String, Integer>();

		String query = "select min(" + columnName + ") as val from " + table_sample;

		Statement stmt = con.createStatement();
		ResultSet rs = stmt.executeQuery(query);
		String min = "";
		while (rs.next()) {
			// freqs.put(rs.getString("val"), rs.getInt("freq"));
			min = rs.getString(1);
		}
		int all = getCount(table_sample);
		int distinct = getCount("(select distinct " + columnName + " from " + table_sample + ")");
		if (distinct == 0) {
			return freqs;
		}
		int freq = all / distinct;
		freqs.put(min, freq);
		rs.close();
		stmt.close();

		return freqs;
	}

	/* inner - helper classes */
	private final class MinMax {
		private final String min;
		private final String max;

		public MinMax(String min, String max) {
			this.min = min;
			this.max = max;
		}

		public String getMin() {
			return min;
		}

		public String getMax() {
			return max;
		}

		@Override
		public String toString() {
			return "MinMax{" + "min=" + min + ", max=" + max + '}';
		}

	}

	private final class ValFreq {
		private final String val;
		private final int freq;

		public ValFreq(String val, int freq) {
			this.val = val;
			this.freq = freq;
		}

		public String getVal() {
			return val;
		}

		public int getFreq() {
			return freq;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime * result + freq;
			result = prime * result + ((val == null) ? 0 : val.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			ValFreq other = (ValFreq) obj;
			if (!getOuterType().equals(other.getOuterType()))
				return false;
			if (freq != other.freq)
				return false;
			if (val == null) {
				if (other.val != null)
					return false;
			} else if (!val.equals(other.val))
				return false;
			return true;
		}

		private Stat getOuterType() {
			return Stat.this;
		}

	}

	public void setSch(String s) {
		this.sch = s;
	}
}

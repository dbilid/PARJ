/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package madgik.exareme.master.queryProcessor.analyzer.stat;

import madgik.exareme.master.queryProcessor.analyzer.fanalyzer.OptiqueAnalyzer;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

/**
 * @author jim
 */
public class ExternalStat implements StatExtractor {
	// public static final int LIMIT = 10000;
	// public static final double LIMIT_FACTOR = 0.3;
	private static final int BLOB_SIZE = 500000;
	private static final int NUM_SIZE = 8;
	private static final int MAX_STRING_SAMPLE = 20;
	// public static final String SAMPLE = "_sample";
	private static final Logger log = Logger.getLogger(ExternalStat.class);

	private final Connection con;
	private String sch;
	private String tblName;

	public ExternalStat(Connection con, String tbl, String schName) {
		sch = schName;
		this.con = con;
		this.tblName = tbl;
	}

	// schema map
	private Map<String, Table> schema = new HashMap<String, Table>();

	@Override
	public Map<String, Table> extractStats() throws Exception {

		DatabaseMetaData dbmd = con.getMetaData(); // dtabase metadata object

		// listing tables and columns
		String catalog = null;
		String schemaPattern = sch;
		String tableNamePattern = tblName;
		String columnNamePattern = "%";
		if (con.getClass().getName().contains("postgresql")) {
			// tableNamePattern="\""+tableNamePattern+"\"";
			schemaPattern = "public";
		}

		// ResultSet resultTables = dbmd.getTables(catalog, "public",
		// tableNamePattern, types);
		ResultSet resultColumns = dbmd.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
		if (con.getClass().getName().contains("postgresql")) {
			tableNamePattern = "\"" + tableNamePattern + "\"";
		} else if (con.getClass().getName().contains("oracle")) {
			tableNamePattern = schemaPattern + "." + tableNamePattern;
		}
		log.debug("Starting extracting stats");
		// while (resultTables.next()) {
		Map<String, Column> columnMap = new HashMap<String, Column>();
		// StringEscapeUtils.escapeJava(resultTables.getString(3));
		log.debug("Analyzing table " + tblName);

		int toupleSize = 0; // in bytes

		// tableNamePattern = tableName;

		int columnCount = resultColumns.getMetaData().getColumnCount();
		Statement st = con.createStatement();
		ResultSet rs = st.executeQuery("select count(*) from " + tableNamePattern);
		int count = 0;
		if (rs.next()) {
			count = rs.getInt(1);
		} else {
			log.error("could not get count for table " + tableNamePattern);
		}
		rs.close();
		st.close();

		ResultSet pkrs = dbmd.getExportedKeys("", "", tblName);
		String pkey = "DEFAULT_KEY";

		while (pkrs.next()) {
			pkey = pkrs.getString("PKCOLUMN_NAME");
			break;
		}
		pkrs.close();
		if (count == 0) {
			log.debug("Empty table");
			Table t = new Table(tblName, columnCount, toupleSize, columnMap, count, pkey);
			schema.put(tblName, t);
			return schema;
		}

		while (resultColumns.next()) {

			String columnName = StringEscapeUtils.escapeJava(resultColumns.getString(4));
			try{
			String colNamePattern = columnName;
			if (con.getClass().getName().contains("postgresql")) {
				colNamePattern = "\"" + columnName + "\"";
			}
			int columnType = resultColumns.getInt(5);

			// computing column's size in bytes
			int columnSize = computeColumnSize(colNamePattern, columnType, tableNamePattern);
			toupleSize += columnSize;

			// execute queries for numberOfDiffValues, minVal, maxVal
			// Map<String, Integer> diffValFreqMap = new HashMap<String,
			// Integer>();

			// computing column's min and max values
			String minVal="0";
			String maxVal="0";
			if (columnType != Types.BLOB){
			MinMax mm = computeMinMax(tableNamePattern, colNamePattern);
			minVal = mm.getMin();
			maxVal = mm.getMax();
			}
			Map<String, Integer> diffValFreqMap=new HashMap<String, Integer>();
			//only for equidepth!
			

			// for (ValFreq k : freqs) {
			// diffValFreqMap.put(k.getVal(), k.getFreq());

			// }

			// /add min max diff vals in the sampling values
			
			int minOcc = 1;
			int maxOcc = 1;
			int diffVals = 0;
			boolean equidepth=false;
			if(equidepth){
				
				//diffValFreqMap is used only in equidepth, do not compute it
				//if we have primitive
				diffValFreqMap = computeDistinctValuesFrequency(tableNamePattern, colNamePattern);
				
				String minValChar = minVal;
				String maxValChar = maxVal;
				if (columnType == Types.VARCHAR || columnType == Types.CHAR || columnType == Types.LONGNVARCHAR
						|| columnType == Types.DATE) {
					minValChar = "\'" + minVal + "\'";
					maxValChar = "\'" + maxVal + "\'";
				}
			try {
				minOcc = computeValOccurences(tableNamePattern, colNamePattern, minValChar);
			} catch (Exception e) {
				log.error(
						"Could not compute value occurences for column:" + colNamePattern + " and value:" + minValChar);
			}
			if (equidepth&&!diffValFreqMap.containsKey(minVal))
				diffValFreqMap.put(minVal, minOcc);
			
			try {
				maxOcc = computeValOccurences(tableNamePattern, colNamePattern, maxValChar);
			} catch (Exception e) {
				log.error(
						"Could not compute value occurences for column:" + colNamePattern + " and value:" + maxValChar);
			}
			if (diffValFreqMap.containsKey(maxVal))
				diffValFreqMap.put(maxVal, maxOcc);

			 diffVals = diffValFreqMap.size();
			}else{
				diffVals=computeDiffVals(tableNamePattern, colNamePattern, columnType);
			}
			if(diffVals==0){
				//all values are null!
				continue;
			}
			Column c = new Column(columnName, columnType, columnSize, diffVals, minVal, maxVal, diffValFreqMap);
			columnMap.put(columnName, c);
			}
				catch(Exception ex){
					log.error("could not analyze column " + columnName + ":" + ex.getMessage());
				}
			
		}

		Table t = new Table(tblName, columnCount, toupleSize, columnMap, count, pkey);
		schema.put(tblName, t);

		// }
		// resultTables.close();
		resultColumns.close();
		return schema;

	}

	private int computeDiffVals(String tableNamePattern, String colNamePattern, int columnType) throws SQLException {
		String query = "select count(*) as freq from (select distinct " + colNamePattern + " from " + tableNamePattern
				+ " where " + colNamePattern + " is not null) A";
		Statement stmt = con.createStatement();
		if (columnType == Types.BLOB) {
			query = "select count(*) as freq from (select " + colNamePattern + " from " + tableNamePattern + " where "
					+ colNamePattern + " is not null) A";
		}
		log.debug("executing distinct values query:" + query);
		ResultSet rs = stmt.executeQuery(query);
		int result = 0;
		while (rs.next()) {
			result = rs.getInt("freq");

		}

		rs.close();
		stmt.close();

		return result;
	}

	/* private-helper methods */
	private int computeColumnSize(String columnName, int columnType, String table_sample) throws Exception {
		int columnSize = 0;
		if (columnType == Types.INTEGER || columnType == Types.REAL || columnType == Types.DOUBLE
				|| columnType == Types.DECIMAL || columnType == Types.FLOAT || columnType == Types.NUMERIC) {
			columnSize = NUM_SIZE;
		} else if (columnType == Types.VARCHAR) {
			String query0 = "select max(length(" + columnName + ")) as length from (select " + columnName + " from "
					+ table_sample + ") A" + " where " + columnName + " is not null limit " + MAX_STRING_SAMPLE;

			if (con.getClass().getName().contains("oracle")) {
				query0 = "select max(length(" + columnName + ")) as length from (select " + columnName + " from "
						+ table_sample + ") A" + " where " + columnName + " is not null and ROWNUM< "
						+ MAX_STRING_SAMPLE;
			}
			log.debug("executing col size query:" + query0);
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
		String query1 = "select min(" + columnName + ") as minVal, max(" + columnName + ") " + "as maxVal  from "
				+ tableName + " where " + columnName + " is not null";

		String minVal = "", maxVal = "";
		log.debug("executing minmax query:" + query1);
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

	private int computeValOccurences(String tableName, String columnName, String value) throws Exception {
		String queryDf = "select count(*) as valCount " + "from " + tableName + " where " + columnName
				+ " is not null and  " + columnName + " = " + value + "";
		log.debug("executing value occur. query:" + queryDf);
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

	private Map<String, Integer> computeDistinctValuesFrequency(String table_sample, String columnName)
			throws Exception {
		Map<String, Integer> freqs = new HashMap<String, Integer>();

		String query = "select " + columnName + " as val, count(*) as freq from " + table_sample + " where "
				+ columnName + " is not null group by " + columnName + "";
		log.debug("executing distinct values freq. query:" + query);
		Statement stmt = con.createStatement();
		ResultSet rs = stmt.executeQuery(query);

		while (rs.next()) {

			freqs.put(rs.getString("val"), rs.getInt("freq"));

		}

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

	public void setSch(String s) {
		this.sch = s;
	}
}

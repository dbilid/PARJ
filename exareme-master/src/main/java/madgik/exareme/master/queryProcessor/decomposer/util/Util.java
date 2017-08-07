/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package madgik.exareme.master.queryProcessor.decomposer.util;

import madgik.exareme.master.queryProcessor.decomposer.query.Operand;

import org.apache.log4j.Logger;

import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author jim
 */
public class Util {

	private static AtomicInteger idCounter = new AtomicInteger();

	private static final Logger log = Logger.getLogger(Util.class);

	private Util() {
	}

	public static int createUniqueId() {
		
			return idCounter.getAndIncrement();
		
	}

	public static boolean operandsAreEqual(Operand op1, Operand op2) {
		if (op1.getClass() == op2.getClass()) {
			return op1.getClass().cast(op1).equals(op2.getClass().cast(op2));
		} else {
			return false;
		}
	}

	public static HashMap<String, HashSet<String>> getMysqlIndices(String conString) throws SQLException {
		HashMap<String, HashSet<String>> result = new HashMap<String, HashSet<String>>();

		Connection conn;

		conn = DriverManager.getConnection(conString);
		DatabaseMetaData meta = conn.getMetaData();
		ResultSet tables = meta.getTables(null, null, null, null);
		while (tables.next()) {
			ResultSet rs;
			String tableName = tables.getString(3);
			rs = meta.getPrimaryKeys(null, null, tableName);
			HashSet<String> hash = new HashSet<String>();
			result.put(tableName.toLowerCase(), hash);
			while (rs.next()) {
				hash.add(rs.getString("COLUMN_NAME"));
				// String columnName = rs.getString("COLUMN_NAME");
				// System.out.println(tables.getString(3));
				// System.out.println("getPrimaryKeys(): columnName=" +
				// columnName);
			}
			rs.close();

		}
		tables.close();
		conn.close();
		return result;
	}




}

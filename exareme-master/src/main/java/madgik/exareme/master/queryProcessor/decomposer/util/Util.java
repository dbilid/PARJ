/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package madgik.exareme.master.queryProcessor.decomposer.util;

import madgik.exareme.master.queryProcessor.decomposer.DecomposerUtils;
import madgik.exareme.master.queryProcessor.decomposer.dag.Node;
import madgik.exareme.master.queryProcessor.decomposer.federation.Memo;
import madgik.exareme.master.queryProcessor.decomposer.federation.MemoKey;
import madgik.exareme.master.queryProcessor.decomposer.query.Column;
import madgik.exareme.master.queryProcessor.decomposer.query.Operand;
import madgik.exareme.master.queryProcessor.decomposer.query.Output;
import madgik.exareme.master.queryProcessor.decomposer.query.SQLQuery;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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


	public static boolean planContainsLargerResult(Node n, Memo finalMemo, double distributedLimit) {
		try {
			double size = n.getNodeInfo().getTupleLength() * n.getNodeInfo().getNumberOfTuples();
			if (size > (distributedLimit * 1000000)) {
				System.out.println("tuple length::::" + n.getNodeInfo().getTupleLength());
				System.out.println("tuple cardinality::::" + n.getNodeInfo().getNumberOfTuples());
				System.out.println("size::::" + size);
				System.out.println(distributedLimit * 1000000);
				return true;
			}
		} catch (java.lang.Exception ex) {
			log.error("could not obtain size estimation for node " + n.getObject().toString());
		}
		if (n.getChildren().isEmpty()) {
			return false;
		}
		if (finalMemo.getMemoValue(new MemoKey(n, null)) == null) {
			return false;
		}
		if (finalMemo.getMemoValue(new MemoKey(n, null)).getPlan() == null) {
			return false;
		}

		Node op = n.getChildAt(finalMemo.getMemoValue(new MemoKey(n, null)).getPlan().getChoice());
		for (Node child : op.getChildren()) {
			if (planContainsLargerResult(child, finalMemo, distributedLimit)) {
				return true;
			}
		}
		return false;
	}

	public static void setDescNotMaterialised(Node e, Memo memo) {
		MemoKey key = new MemoKey(e, null);
		memo.getMemoValue(key).setMaterialized(false);
		if (e.getChildren().isEmpty()) {
			return;
		}
		Node op = e.getChildAt(memo.getMemoValue(key).getPlan().getChoice());
		for (Node e2 : op.getChildren()) {
			setDescNotMaterialised(e2, memo);
		}
	}

	public static List<Output> getOutputForTable(String sql, String name) {
		Connection memory;
		List<Output> result = new ArrayList<Output>();
		try {
			memory = DriverManager.getConnection("jdbc:sqlite::memory:");

			Statement st = memory.createStatement();
			st.execute(sql);
			ResultSet re = memory.getMetaData().getColumns(null, null, name, "%");
			while (re.next()) {
				result.add(new Output(re.getString(4), new Column(name, re.getString(4))));
			}
			re.close();
			st.close();
			memory.close();
		} catch (SQLException e) {
			log.debug(e.getMessage());
			return result;
		}
		return result;

	}

}

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
import madgik.exareme.master.queryProcessor.decomposer.query.SQLQuery;
import madgik.exareme.master.queryProcessor.decomposer.query.SQLQueryParser;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jim
 */
public class Util {

	private static AtomicLong idCounter = new AtomicLong();

	private static final Logger log = Logger.getLogger(Util.class);

	private Util() {
	}

	public static String createUniqueId() {
		if (DecomposerUtils.RANDOM_TABLENAME_GENERATION) {
			return UUID.randomUUID().toString().replace("-", "");
		} else {
			return String.valueOf(idCounter.getAndIncrement());
		}
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

	public static Set<String> getAnalyzeColumns(List<String> queries) {
		Map<String, Set<String>> tables = new HashMap<String, Set<String>>();
		for (String q : queries) {
			SQLQuery query = new SQLQuery();
			try {
				query = SQLQueryParser.parse(q, null);
			} catch (Exception e) {
				log.error("Could not generate analyze commands." + e.getMessage());
			}
			for (Column c : query.getAllReferencedColumns()) {
				if (!tables.containsKey(c.getAlias())) {
					tables.put(c.getAlias(), new HashSet<String>());
				}
				Set<String> cols = tables.get(c.getAlias());
				cols.add(c.getName());

			}
		}
		Set<String> result = new HashSet<String>();
		for (String tablename : tables.keySet()) {
			String analyze = "analyzeTable " + tablename;
			for (String col : tables.get(tablename)) {
				analyze += " " + col;
			}
			result.add(analyze);

		}
		return result;
	}

	public static boolean planContainsLargerResult(Node n, Memo finalMemo, double distributedLimit) {
		try {
			double size=n.getNodeInfo().getTupleLength() * n.getNodeInfo().getNumberOfTuples();
			if (size > (distributedLimit * 1000000)) {
				System.out.println("tuple length::::"+n.getNodeInfo().getTupleLength());
				System.out.println("tuple cardinality::::"+ n.getNodeInfo().getNumberOfTuples());
				System.out.println("size::::"+size);
				System.out.println(distributedLimit * 1000000);
				return true;
			}
		} catch (java.lang.Exception ex) {
			log.error("could not obtain size estimation for node " + n.getObject().toString());
		}
		if(n.getChildren().isEmpty()){
			return false;
		}
        if (finalMemo.getMemoValue(new MemoKey(n, null))==null) {
            return false;
        }
        if (finalMemo.getMemoValue(new MemoKey(n, null)).getPlan()==null){
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
		MemoKey key=new MemoKey(e , null);
		memo.getMemoValue(key).setMaterialized(false);
		if(e.getChildren().isEmpty()){
			return;
		}
		Node op=e.getChildAt(memo.getMemoValue(key).getPlan().getChoice());
		for(Node e2:op.getChildren()){
			setDescNotMaterialised(e2, memo);
		}
	}

}

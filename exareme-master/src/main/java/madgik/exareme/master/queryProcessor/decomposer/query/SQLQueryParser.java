/**
 * Copyright MaDgIK Group 2010 - 2015.
 */
package madgik.exareme.master.queryProcessor.decomposer.query;

import com.foundationdb.sql.parser.StatementNode;

import madgik.exareme.master.queryProcessor.decomposer.dag.NodeHashValues;
import madgik.exareme.master.queryProcessor.decomposer.federation.DistSQLParser;
import madgik.exareme.master.queryProcessor.decomposer.federation.NamesToAliases;
import madgik.exareme.master.queryProcessor.decomposer.query.visitors.SQLQueryVisitor;

/**
 * @author heraldkllapi
 */
public class SQLQueryParser {
    public static SQLQuery parse(String queryString, NodeHashValues hashes) throws Exception {
        DistSQLParser parser = new DistSQLParser();
        StatementNode node = parser.parseStatement(queryString);
        //node.treePrint();
        // Traverse the qury tree
        SQLQuery query = new SQLQuery();
        //query.readDBInfo();
        SQLQueryVisitor visitor = new SQLQueryVisitor(query, hashes);
        node.accept(visitor);
        return query;
    }

	public static SQLQuery parse(String queryString, NodeHashValues hashes, NamesToAliases n2a) throws Exception {
		DistSQLParser parser = new DistSQLParser();
        StatementNode node = parser.parseStatement(queryString);
        //node.treePrint();
        // Traverse the qury tree
        SQLQuery query = new SQLQuery();
        //query.readDBInfo();
        SQLQueryVisitor visitor = new SQLQueryVisitor(query, hashes, n2a);
        node.accept(visitor);
        return query;
	}
}

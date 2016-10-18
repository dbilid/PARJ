/**
 * Copyright MaDgIK Group 2010 - 2015.
 */
package madgik.exareme.master.queryProcessor.decomposer.query;

import java.util.Map;
import java.util.Set;

import com.foundationdb.sql.parser.DeleteNode;
import com.foundationdb.sql.parser.DropTableNode;
import com.foundationdb.sql.parser.InsertNode;
import com.foundationdb.sql.parser.ResultSetNode;

import com.foundationdb.sql.parser.StatementNode;

import madgik.exareme.master.queryProcessor.decomposer.dag.NodeHashValues;
import madgik.exareme.master.queryProcessor.decomposer.federation.DistSQLParser;
import madgik.exareme.master.queryProcessor.decomposer.federation.NamesToAliases;
import madgik.exareme.master.queryProcessor.decomposer.query.visitors.SQLQueryVisitor;

/**
 * @author heraldkllapi
 */
public class SQLQueryParser {
    public static SQLQuery parse(String queryString, NodeHashValues hashes)
        throws Exception {

        DistSQLParser parser = new DistSQLParser();
        StatementNode node = parser.parseStatement(queryString);
        // node.treePrint();
        // Traverse the qury tree
        SQLQuery query = new SQLQuery();
        if (node instanceof InsertNode) {
            InsertNode in = (InsertNode) node;
            ResultSetNode a = in.getResultSetNode();
            SQLQueryVisitor visitor = new SQLQueryVisitor(query, hashes);
            a.accept(visitor);
            query.setTemporaryTableName(in.getTargetTableName().getTableName());
            return query;
        } else if (node instanceof DropTableNode) {
            DropTableNode del = (DropTableNode) node;
            query.setDrop(true);
            query.setTemporaryTableName(del.getRelativeName());
            return query;
        } else {
            // query.readDBInfo();
            SQLQueryVisitor visitor = new SQLQueryVisitor(query, hashes);
            node.accept(visitor);
            return query;
        }
    }

    public static SQLQuery parse(String queryString, NodeHashValues hashes,
                                 NamesToAliases n2a, Map<String, Set<String>> refCols)
        throws Exception {

        DistSQLParser parser = new DistSQLParser();
        StatementNode node = parser.parseStatement(queryString);
        // node.treePrint();
        // Traverse the qury tree
        // query.readDBInfo();
        SQLQuery query = new SQLQuery();

        if (node instanceof InsertNode) {
            InsertNode in = (InsertNode) node;
            ResultSetNode a = in.getResultSetNode();
            SQLQueryVisitor visitor = new SQLQueryVisitor(query, hashes, n2a,
                    refCols);
            a.accept(visitor);
            query.setTemporaryTableName(in.getTargetTableName().getTableName());
            return query;
        } else if (node instanceof DropTableNode) {
            DropTableNode del = (DropTableNode) node;
            query.setDrop(true);
            query.setTemporaryTableName(del.getRelativeName());
            return query;
        } else {
            SQLQueryVisitor visitor = new SQLQueryVisitor(query, hashes, n2a,
                refCols);
            node.accept(visitor);
            return query;
        }
	}
}

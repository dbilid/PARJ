/**
 * Copyright MaDgIK Group 2010 - 2015.
 */
package madgik.exareme.master.queryProcessor.decomposer.query.visitors;

import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.parser.*;

import madgik.exareme.master.queryProcessor.decomposer.dag.Node;
import madgik.exareme.master.queryProcessor.decomposer.dag.NodeHashValues;
import madgik.exareme.master.queryProcessor.decomposer.federation.NamesToAliases;
import madgik.exareme.master.queryProcessor.decomposer.query.Column;
import madgik.exareme.master.queryProcessor.decomposer.query.NonUnaryWhereCondition;
import madgik.exareme.master.queryProcessor.decomposer.query.Operand;
import madgik.exareme.master.queryProcessor.decomposer.query.QueryUtils;
import madgik.exareme.master.queryProcessor.decomposer.query.SQLQuery;
import madgik.exareme.master.queryProcessor.decomposer.query.Table;
import madgik.exareme.master.queryProcessor.decomposer.util.Util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

/**
 * @author heraldkllapi
 */
public class SQLQueryVisitor extends AbstractVisitor {

    private boolean stop = false;
    private NodeHashValues hashes;
    private NamesToAliases n2a;

    public SQLQueryVisitor(SQLQuery query, NodeHashValues h) {
        super(query);
        hashes=h;
    }

    public SQLQueryVisitor(SQLQuery query, NodeHashValues h, NamesToAliases n2a) {
    	super(query);
        hashes=h;
        this.n2a=n2a;
	}

	@Override public Visitable visit(Visitable node) throws StandardException {


        if (node instanceof JoinNode) {
            if (query.getJoinNode() == null) {
            	Map<String, Integer> counts = new HashMap<String, Integer>();
            	query.setJoinNode(getJoinNode((JoinNode) node, counts, new HashMap<String, String>()));
                //decomposeJoinNode((JoinNode) node);
                //WhereClauseVisitor whereVisitor = new WhereClauseVisitor(query);
                //node.accept(whereVisitor);
            }

        }

        if (node instanceof UnionNode) {
            decomposeUnionNode((UnionNode) node);
            this.query.setHasUnionRootNode(true);
            stop = true;
        }
        if (node instanceof CursorNode) {
            CursorNode cNode = (CursorNode) node;
            if (cNode.getFetchFirstClause() != null) {
                query.setLimit(
                    (int) (Integer) ((ConstantNode) cNode.getFetchFirstClause()).getValue());
            }
            if (cNode.getResultSetNode() instanceof UnionNode) {
                //top node is a union
                UnionNode uNode = (UnionNode) cNode.getResultSetNode();

                decomposeUnionNode(uNode);
                this.query.setHasUnionRootNode(true);
                stop = true;
            }

        }
        if (node instanceof SelectNode) {
            SelectVisitor selectVis = new SelectVisitor(query);
            node.accept(selectVis);
        }
        if (node instanceof OrderByList) {
            OrderByVisitor orderVisitor = new OrderByVisitor(query);
            node.accept(orderVisitor);
        }

        if (node instanceof FromList) {
            //check if we have nested subquery (union or nested select)
            FromList fl = (FromList) node;
            for (int i = 0; i < fl.size(); i++) {
                if (fl.get(i) instanceof FromSubquery) {
                    FromSubquery from = (FromSubquery) fl.get(i);
                    String alias = from.getCorrelationName();
                    ResultSetNode rs = from.getSubquery();
                    if (rs instanceof UnionNode) {
                        UnionNode uNode = (UnionNode) rs;
                        decomposeUnionNode(uNode);
                        this.query.setUnionAlias(alias);

                    }
                    if (rs instanceof SelectNode) {
                        //nested select
                        SelectNode nestedSelectNode = (SelectNode) rs;

                        SQLQuery nestedSelectSubquery = new SQLQuery();
                        //query.readDBInfo();
                        SQLQueryVisitor subqueryVisitor = new SQLQueryVisitor(nestedSelectSubquery, hashes, n2a);
                        nestedSelectNode.accept(subqueryVisitor);

                        this.query.addNestedSelectSubquery(nestedSelectSubquery, alias);
                        //;.nestedSelectSubquery = nestedSelectSubquery;
                        //this.query.setNestedSelectSubqueryAlias(alias);
                    }
                }
            }
            //   if (fl.get(0) instanceof JoinNode) {
            //        JoinNode jNode = (JoinNode) fl.get(0);
            //        decomposeJoinNode(jNode);
            //    }


        }



        // Limit
        //    if ().getFetchFirstClause()
        return node;
    }

    private Node getJoinNode(JoinNode node, Map<String, Integer> counts, Map<String, String> correspondingAliases) {
		Node j=new Node(Node.AND, Node.JOIN);
		if(node instanceof HalfOuterJoinNode){
			HalfOuterJoinNode outer=(HalfOuterJoinNode)node;
			if(outer.isRightOuterJoin()){
				j.setOperator(Node.RIGHTJOIN);
			}
			else{
				j.setOperator(Node.LEFTJOIN);
			}
		}
		ResultSetNode left=node.getLogicalLeftResultSet();
		if(left instanceof FromBaseTable){
			FromBaseTable bt=(FromBaseTable)left;
			String originalAlias=bt.getExposedName();
			String tblName=bt.getOrigTableName().getTableName();
			String newAlias=originalAlias;
			if (counts.containsKey(tblName)) {
				counts.put(tblName, counts.get(tblName) + 1);
				newAlias=n2a.getGlobalAliasForBaseTable(tblName, counts.get(tblName));
			} else {
				counts.put(tblName, 0);
				newAlias=n2a.getGlobalAliasForBaseTable(tblName, 0);
			}
			Table t=new Table(tblName, newAlias);
			Node table = new Node(Node.OR);
			table.addDescendantBaseTable(t.getAlias());
			correspondingAliases.put(originalAlias, newAlias);
			table.setObject(t);
			if (!hashes.containsKey(table.getHashId())) {

				// table.setHashID(Objects.hash(t.getName()));
				// table.setIsBaseTable(true);
				hashes.put(table.getHashId(), table);
			} else {
				table = hashes.get(table.getHashId());
			}
			j.addChild(table);
			j.addAllDescendantBaseTables(table.getDescendantBaseTables());
		}
		else if(left instanceof JoinNode){
			Node n=getJoinNode((JoinNode)left, counts, correspondingAliases);
			j.addChild(n);
			j.addAllDescendantBaseTables(n.getDescendantBaseTables());
		}
		else{
			System.err.println("error in join, unknown child type");
		}
		
		ResultSetNode right=node.getLogicalRightResultSet();
		if(right instanceof FromBaseTable){
			FromBaseTable bt=(FromBaseTable)right;
			String originalAlias=bt.getExposedName();
			String tblName=bt.getOrigTableName().getTableName();
			String newAlias=originalAlias;
			if (counts.containsKey(tblName)) {
				counts.put(tblName, counts.get(tblName) + 1);
				newAlias=n2a.getGlobalAliasForBaseTable(tblName, counts.get(tblName));
			} else {
				counts.put(tblName, 0);
				newAlias=n2a.getGlobalAliasForBaseTable(tblName, 0);
			}
			Table t=new Table(tblName, newAlias);
			Node table = new Node(Node.OR);
			table.addDescendantBaseTable(t.getAlias());
			correspondingAliases.put(originalAlias, newAlias);
			table.setObject(t);
			if (!hashes.containsKey(table.getHashId())) {

				// table.setHashID(Objects.hash(t.getName()));
				// table.setIsBaseTable(true);
				hashes.put(table.getHashId(), table);
			} else {
				table = hashes.get(table.getHashId());
			}
			j.addChild(table);
			j.addAllDescendantBaseTables(table.getDescendantBaseTables());
		}
		else if(right instanceof JoinNode){
			Node n=getJoinNode((JoinNode)right, counts, correspondingAliases);
			j.addChild(n);
			j.addAllDescendantBaseTables(n.getDescendantBaseTables());
		}
		else{
			System.err.println("error in join, unknown child type");
		}
		//List<Operand> joinConditions=new ArrayList<Operand>();
		Operand op=QueryUtils.getOperandFromNode(node.getJoinClause());
		for(Column c:op.getAllColumnRefs()){
			op.changeColumn(c, new Column(correspondingAliases.get(c.getAlias()), c.getColumnName()));
		}
		j.setObject(op);
		
		
		
		Node parent = new Node(Node.OR);

		Table selt = new Table("table" + Util.createUniqueId(), null);
		parent.setObject(selt);
		parent.addChild(j);

		if (!hashes.containsKey(parent.getHashId())) {
			parent.addAllDescendantBaseTables(j.getDescendantBaseTables());
			hashes.put(parent.getHashId(), parent);
			// selection.addChild(table);

		} else {
			parent = hashes.get(parent.getHashId());

		}
		
		return parent;
	}

	@Override public boolean skipChildren(Visitable node) {
        return FromSubquery.class.isInstance(node) || (node instanceof JoinNode
            && query.getJoinType() != null);
    }

    @Override public boolean stopTraversal() {
        return stop;
    }

    private void decomposeUnionNode(UnionNode uNode) throws StandardException {
        SQLQuery leftSubquery = new SQLQuery();
        SQLQuery rightSubquery = new SQLQuery();
        //query.readDBInfo();
        SQLQueryVisitor leftVisitor = new SQLQueryVisitor(leftSubquery, hashes, n2a);
        SQLQueryVisitor rightVisitor = new SQLQueryVisitor(rightSubquery, hashes, n2a);

        if (uNode.getResultColumns() != null) {
            //uNode.getResultColumns().accept(leftVisitor);
            //uNode.getResultColumns().accept(rightVisitor);
        }
        if (uNode.getLeftResultSet() != null) {
            // uNode.getLeftResultSet().treePrint();
            if (uNode.getLeftResultSet() instanceof UnionNode) {
                decomposeUnionNode((UnionNode) uNode.getLeftResultSet());
            } else if (uNode.getLeftResultSet() instanceof JoinNode) {
                SQLQueryVisitor v = new SQLQueryVisitor(leftSubquery, hashes, n2a);
                uNode.getLeftResultSet().accept(v);
                this.query.getUnionqueries().add(leftSubquery);
            } else {
                uNode.getLeftResultSet().accept(leftVisitor);
                this.query.getUnionqueries().add(leftSubquery);
            }
        }
        if (uNode.getRightResultSet() != null) {
            // uNode.getLeftResultSet().treePrint();
            if (uNode.getRightResultSet() instanceof UnionNode) {
                decomposeUnionNode((UnionNode) uNode.getRightResultSet());
            } else if (uNode.getRightResultSet() instanceof JoinNode) {
                SQLQueryVisitor v = new SQLQueryVisitor(rightSubquery, hashes, n2a);
                uNode.getRightResultSet().accept(v);
                this.query.getUnionqueries().add(rightSubquery);
            } else {
                uNode.getRightResultSet().accept(rightVisitor);
                this.query.getUnionqueries().add(rightSubquery);
            }

        }
        //uNode.accept(visitor);
        this.query.setUnionAll(uNode.isAll());

    }

    private void decomposeJoinNode(JoinNode jNode) throws StandardException {
        SQLQuery leftSubquery = new SQLQuery();
        SQLQuery rightSubquery = new SQLQuery();
        //query.readDBInfo();


        //   if (jNode.getResultColumns() != null) {
        //uNode.getResultColumns().accept(leftVisitor);
        //uNode.getResultColumns().accept(rightVisitor);
        //   }
        if (jNode.getLeftResultSet() != null) {
            // for now we only consider that the join operators are base tables or nested joins
            if (jNode.getLeftResultSet() instanceof FromSubquery) {
                FromSubquery fs = (FromSubquery) jNode.getLeftResultSet();
                SQLQueryVisitor v = new SQLQueryVisitor(leftSubquery, hashes, n2a);
                fs.getSubquery().accept(v);
                this.query.setLeftJoinTableAlias(fs.getCorrelationName());
                //jNode.getLeftResultSet().accept(leftVisitor);
            } else if (jNode.getLeftResultSet() instanceof JoinNode) {
                SQLQueryVisitor v = new SQLQueryVisitor(leftSubquery, hashes, n2a);
                jNode.getLeftResultSet().accept(v);
                //leftSubquery.setSelectAll(true);
            } else if (jNode.getLeftResultSet() instanceof FromBaseTable) {



                FromBaseTableVisitor v = new FromBaseTableVisitor(leftSubquery);
                jNode.getLeftResultSet().accept(v);
                //leftSubquery.setSelectAll(true);
                //DO WE NEED EXTRA QUERY FOR EACH BASE TABLE????
                //FromBaseTable bt=(FromBaseTable) jNode.getLeftResultSet();
                leftSubquery.setIsBaseTable(true);
                //leftSubquery.setResultTableName(bt.getCorrelationName());
            }
            /* for now we only consider that the join operators are base tables or nested joins
             else {
             SQLQueryVisitor v=new SQLQueryVisitor(leftSubquery);
             jNode.getLeftResultSet().accept(v);
             }*/

            //System.out.println("Table "+query.getResultTableName()+" add left join table: "+leftSubquery.getResultTableName());
            this.query.setLeftJoinTable(leftSubquery);

        }
        /*if (jNode.getRightResultSet() != null) {
         // uNode.getLeftResultSet().treePrint();
         if (jNode.getRightResultSet() instanceof UnionNode) {
         decomposeUnionNode((UnionNode) jNode.getRightResultSet());
         } else if (jNode.getRightResultSet() instanceof JoinNode) {
         decomposeJoinNode((JoinNode) jNode.getRightResultSet());
         } else {
         if(jNode.getRightResultSet() instanceof FromSubquery){
         this.query.rightJoinTableAlias=((FromSubquery)jNode.getRightResultSet()).getCorrelationName();}
                
         jNode.getRightResultSet().accept(rightVisitor);
         this.query.rightJoinTable = rightSubquery;
         }
         }*/

        if (jNode.getRightResultSet() != null) {

            //for now we only consider that the join operators are base tables or nested joins
            if (jNode.getRightResultSet() instanceof FromSubquery) {
                FromSubquery fs = (FromSubquery) jNode.getRightResultSet();
                SQLQueryVisitor v = new SQLQueryVisitor(rightSubquery, hashes, n2a);
                fs.getSubquery().accept(v);
                this.query.setRightJoinTableAlias(fs.getCorrelationName());
                //jNode.getLeftResultSet().accept(leftVisitor);
            } else if (jNode.getRightResultSet() instanceof JoinNode) {
                SQLQueryVisitor v = new SQLQueryVisitor(rightSubquery, hashes, n2a);
                jNode.getRightResultSet().accept(v);
                //rightSubquery.setSelectAll(true);
            } else if (jNode.getRightResultSet() instanceof FromBaseTable) {
                //FromBaseTable bt=(FromBaseTable)jNode.getRightResultSet();
                FromBaseTableVisitor v = new FromBaseTableVisitor(rightSubquery);
                jNode.getRightResultSet().accept(v);
                //rightSubquery.setSelectAll(true);
                rightSubquery.setIsBaseTable(true);
                //rightSubquery.setResultTableName(bt.getCorrelationName());
            }
            /* for now we only consider that the join operators are base tables or nested joins
             else {
             SQLQueryVisitor v=new SQLQueryVisitor(rightSubquery);
             jNode.getRightResultSet().accept(v);
             }*/
            //System.out.println("Table "+query.getResultTableName()+" add right join table: "+rightSubquery.getResultTableName());
            this.query.setRightJoinTable(rightSubquery);

        }
        if (jNode instanceof HalfOuterJoinNode) {
            if (!((HalfOuterJoinNode) jNode).isRightOuterJoin()) {
                this.query.setJoinType("left outer join");
            } else {
                this.query.setJoinType("right outer join");
            }
        } else {
            //we only have joins and left outer joins to add condition to check!
            this.query.setJoinType("join");
        }
        //uNode.accept(visitor);

    }
}

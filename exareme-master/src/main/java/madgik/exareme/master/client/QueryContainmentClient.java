package madgik.exareme.master.client;

import madgik.exareme.master.engine.intermediateCache.QueryContainment;
import madgik.exareme.master.queryProcessor.decomposer.dag.NodeHashValues;
import madgik.exareme.master.queryProcessor.decomposer.query.SQLQuery;
import madgik.exareme.master.queryProcessor.decomposer.query.SQLQueryParser;

/**
 * Created by christos on 12/4/2016.
 */
public class QueryContainmentClient {

    public static void main(String[] args) throws Exception {

//        String query = "select * from A, B, C where A.x>=1 and A.x<=5 and B.x=5 and C.x<8 and A.z>=2 and A.z<=7 ";
//        System.out.println("query "+query);
//        SQLQuery sqlQuery1 = SQLQueryParser.parse(query, new NodeHashValues());
//
//        query = "select * from A, B, C where A.x>=2 and A.x<=4 and B.x=5 and C.x<=6 and A.y>=3 and A.y<=7 ";
//        System.out.println("query "+query);
//        SQLQuery sqlQuery2 = SQLQueryParser.parse(query, new NodeHashValues());
//
//        QueryContainment qc = new QueryContainment(sqlQuery1, sqlQuery2);
//        String result = qc.containQuery();
//        System.out.println("\n!!!!!result is " + result + "\n\n");
//        qc.clear();
//
//        query = "select * from A, B, C where A.x>=1 and A.x<=5 and B.x=5 and C.x<8 and A.z>=2 and A.z<=7 ";
//        System.out.println("query "+query);
//        sqlQuery1 = SQLQueryParser.parse(query, new NodeHashValues());
//
//        query = "select * from A, B where A.x>=2 and A.x<=4 and B.x=5 and A.y>=3 and A.y<=7 ";
//        System.out.println("query "+query);
//        sqlQuery2 = SQLQueryParser.parse(query, new NodeHashValues());
//
//        qc = new QueryContainment(sqlQuery1, sqlQuery2);
//        result = qc.containQuery();
//        System.out.println("\n!!!!!result is "+result+"\n\n");
//        qc.clear();
//
//        query = "select * from A, B, C where A.x>=1 and A.x<=5 and B.x=5 ";
//        System.out.println("query "+query);
//        sqlQuery1 = SQLQueryParser.parse(query, new NodeHashValues());
//
//        query = "select * from A, B, C where A.x>=2 and A.x<=4 and B.x=5 and A.y>=3 and A.y<=7 ";
//        System.out.println("query "+query);
//        sqlQuery2 = SQLQueryParser.parse(query, new NodeHashValues());
//
//        qc = new QueryContainment(sqlQuery1, sqlQuery2);
//        result = qc.containQuery();
//        System.out.println("\n!!!!!result is "+result+"\n\n");
//        qc.clear();
//
//        query = "select * from A, B, C where A.x>=1 and A.x<=5 and B.x=5 AND A.id>=B.id ";
//        System.out.println("query "+query);
//        sqlQuery1 = SQLQueryParser.parse(query, new NodeHashValues());
//
//        query = "select * from A, B, C where A.x>=2 and A.x<=4 and B.x=5 and A.y>=3 and A.y<=7 ";
//        System.out.println("query "+query);
//        sqlQuery2 = SQLQueryParser.parse(query, new NodeHashValues());
//
//        qc = new QueryContainment(sqlQuery1, sqlQuery2);
//        result = qc.containQuery();
//        System.out.println("\n!!!!!result is "+result+"\n\n");
//        qc.clear();
//
//        query = "select * from A, B, C where A.x>=1 and A.x<=5 and B.x=5 AND A.id>=B.id ";
//        System.out.println("query "+query);
//        sqlQuery1 = SQLQueryParser.parse(query, new NodeHashValues());
//
//        query = "select * from A, B, C where A.x>=2 and A.x<=5 and B.x=5 and A.y>=3 and A.y<=7 AND B.id=A.id";
//        System.out.println("query "+query);
//        sqlQuery2 = SQLQueryParser.parse(query, new NodeHashValues());
//
//        qc = new QueryContainment(sqlQuery1, sqlQuery2);
//        result = qc.containQuery();
//        System.out.println("\n!!!!!result is "+result+"\n\n");
//        qc.clear();
//
//        query = "select * from A, B, C where A.x>=1 and A.x<=5 and B.x=5 AND A.id>=B.id ";
//        System.out.println("query "+query);
//        sqlQuery1 = SQLQueryParser.parse(query, new NodeHashValues());
//
//        query = "select * from A, B, C where A.x>=2 and A.x<=4 and B.x=5 and A.y>=3 and A.y<=7 AND A.id>B.id";
//        System.out.println("query "+query);
//        sqlQuery2 = SQLQueryParser.parse(query, new NodeHashValues());
//
//        qc = new QueryContainment(sqlQuery1, sqlQuery2);
//        result = qc.containQuery();
//        System.out.println("\n!!!!!result is "+result+"\n\n");
//        qc.clear();
//
//        query = "select * from A, B";
//        System.out.println("query "+query);
//        sqlQuery1 = SQLQueryParser.parse(query, new NodeHashValues());
//
//        query = "select * from A, B, C where A.id=B.id and C.a=8";
//        System.out.println("query "+query);
//        sqlQuery2 = SQLQueryParser.parse(query, new NodeHashValues());
//
//        qc = new QueryContainment(sqlQuery1, sqlQuery2);
//        result = qc.containQuery();
//        System.out.println("\n!!!!!result is "+result+"\n\n");
//        qc.clear();
//
//        query = "select * from A, B where A.id=B.id";
//        System.out.println("query "+query);
//        sqlQuery1 = SQLQueryParser.parse(query, new NodeHashValues());
//
//        query = "select * from A, B, C where A.id=B.id and C.a=8";
//        System.out.println("query "+query);
//        sqlQuery2 = SQLQueryParser.parse(query, new NodeHashValues());
//
//        qc = new QueryContainment(sqlQuery1, sqlQuery2);
//        result = qc.containQuery();
//        System.out.println("\n!!!!!result is "+result+"\n\n");
//        qc.clear();
//
//        query = "select a.id as RenameA from A a, A b where a.id=b.id";
//        System.out.println("query "+query);
//        sqlQuery1 = SQLQueryParser.parse(query, new NodeHashValues());
//
//        query = "select * from A a, A b, C c where a.id=b.id and C.a=8";
//        System.out.println("query "+query);
//        sqlQuery2 = SQLQueryParser.parse(query, new NodeHashValues());
//
//        qc = new QueryContainment(sqlQuery1, sqlQuery2);
//        result = qc.containQuery();
//        System.out.println("\n!!!!!result is "+result+"\n\n");
//        qc.clear();
//
//        System.out.println("arxi");
//        qc = new QueryContainment(sqlQuery2);
//
//        query = "select * from A, B, C where A.x>=1 and A.x<=5 and B.x=5 AND A.id>=B.id ";
//        System.out.println("query "+query);
//        sqlQuery1 = SQLQueryParser.parse(query, new NodeHashValues());
//        qc.setCachedQuery(sqlQuery1);
//
//        query = "select a.id as RenameA from A a, A b where a.id=b.id";
//        System.out.println("query "+query);
//        sqlQuery1 = SQLQueryParser.parse(query, new NodeHashValues());
//        qc.setCachedQuery(sqlQuery1);
//
//        result = qc.containQuery();
//        System.out.println("\n!!!!!result is "+result+"\n\n");
//
//        System.out.println("arxi");
//        qc = new QueryContainment(sqlQuery2);
//
//        query = "select a.id as RenameA from A a, A b where a.id=b.id";
//        System.out.println("query "+query);
//        sqlQuery1 = SQLQueryParser.parse(query, new NodeHashValues());
//        qc.setCachedQuery(sqlQuery1);
//
//        query = "select * from A a, B, C where a.x>=1 and a.x<=5 and B.x=5 AND a.id>=B.id ";
//        System.out.println("query "+query);
//        sqlQuery1 = SQLQueryParser.parse(query, new NodeHashValues());
//        qc.setCachedQuery(sqlQuery1);
//
//
//        result = qc.containQuery();
//        System.out.println("\n!!!!!result is "+result+"\n\n");
//        qc.clear();
//
//        query = "select * from A a where a.id<=7 and a.id>=3";
//        System.out.println("query "+query);
//        sqlQuery1 = SQLQueryParser.parse(query, new NodeHashValues());
//
//        query = "select * from A a where a.id=3 and a.id=6 and a.id=7";
//        System.out.println("query "+query);
//        sqlQuery2 = SQLQueryParser.parse(query, new NodeHashValues());
//
//        System.out.println("new query");
//        qc = new QueryContainment(sqlQuery1, sqlQuery2);
//        result = qc.containQuery();
//        System.out.println("\n!!!!!result is " + result + "\n\n");
//        qc.clear();
//
//        query = "select * from A a where a.id<=7 and a.id>=3";
//        System.out.println("query "+query);
//        sqlQuery1 = SQLQueryParser.parse(query, new NodeHashValues());
//
//        query = "select * from A a where a.id<6 and a.id>=3 and a.id=155";
//        System.out.println("query "+query);
//        sqlQuery2 = SQLQueryParser.parse(query, new NodeHashValues());
//
//        System.out.println("new query");
//        qc = new QueryContainment(sqlQuery1, sqlQuery2);
//        result = qc.containQuery();
//        System.out.println("\n!!!!!result is " + result + "\n\n");
//        qc.clear();
//
//        query = "select * from A a where a.id<=7 and a.id>=3";
//        System.out.println("query "+query);
//        sqlQuery1 = SQLQueryParser.parse(query, new NodeHashValues());
//
//        query = "select * from A a where a.id<=9  and a.id>=8";
//        System.out.println("query "+query);
//        sqlQuery2 = SQLQueryParser.parse(query, new NodeHashValues());
//
//        System.out.println("new query");
//        qc = new QueryContainment(sqlQuery1, sqlQuery2);
//        result = qc.containQuery();
//        System.out.println("\n!!!!!result is " + result + "\n\n");
//        qc.clear();
//
//        query = "select * from A a where a.id<=7 and a.id>=3";
//        System.out.println("query "+query);
//        sqlQuery1 = SQLQueryParser.parse(query, new NodeHashValues());
//
//        query = "select * from A b where b.id<=7  and b.id>=3";
//        System.out.println("query "+query);
//        sqlQuery2 = SQLQueryParser.parse(query, new NodeHashValues());
//
//        System.out.println("new query");
//        qc = new QueryContainment(sqlQuery1, sqlQuery2);
//        result = qc.containQuery();
//        System.out.println("\n!!!!!result is " + result + "\n\n");
//        qc.clear();
//
//        query = "select * from A a where a.id<=7";
//        System.out.println("query "+query);
//        sqlQuery1 = SQLQueryParser.parse(query, new NodeHashValues());
//
//        query = "select * from A a where a.id=5";
//        System.out.println("query "+query);
//        sqlQuery2 = SQLQueryParser.parse(query, new NodeHashValues());
//
//        System.out.println("new query");
//        qc = new QueryContainment(sqlQuery1, sqlQuery2);
//        result = qc.containQuery();
//        System.out.println("\n!!!!!result is " + result + "\n\n");
//        qc.clear();
//
//        query = "select a.id as a_id, b.id as b_id from A a, B b where a.id=b.id and a.id>=5";
//        System.out.println("query "+query);
//        sqlQuery1 = SQLQueryParser.parse(query, new NodeHashValues());
//        qc.setCachedQuery(sqlQuery1);
//
//        query = "select * from A a, B b, C c where a.x>=1 and B.y=5 and a.id>=5 AND a.id=b.id and a.id=c.id";
//        System.out.println("query "+query);
//        sqlQuery2 = SQLQueryParser.parse(query, new NodeHashValues());
//        qc.setDemandedQuery(sqlQuery2);
//
//        result = qc.containQuery();
//        System.out.println("\n!!!!!result is "+result+"\n\n");
//        qc.clear();



        String query;
        SQLQuery sqlQuery1, sqlQuery2;
        QueryContainment qc = new QueryContainment();
        String result;

        System.out.println("\n\nApo edw dokimes!\n\n");

        query = "SELECT * FROM A a WHERE a.id >= 2";
        System.out.println("query "+query);
        sqlQuery1 = SQLQueryParser.parse(query, new NodeHashValues());
        qc.setCachedQuery(sqlQuery1, "myTable");

        query = "SELECT * FROM A a WHERE a.id=4 AND a.id=7";
        System.out.println("query "+query);
        sqlQuery2 = SQLQueryParser.parse(query, new NodeHashValues());
        qc.setDemandedQuery(sqlQuery2);

        result = qc.containQuery();
        System.out.println("\n!!!!!result is "+result+"\n\n");
        qc.clear();
        //Telos 11111111111111111111111111111111111111111


        query = "SELECT a.x AS a_x, a.y AS a_y, a.z AS a_z FROM A a, B b WHERE a.x>=1 AND a.x<=5 AND b.x=5  ";
        System.out.println("query "+query);
        sqlQuery1 = SQLQueryParser.parse(query, new NodeHashValues());
        qc.setCachedQuery(sqlQuery1, "myTable");

        query = "SELECT * FROM A a, B b WHERE a.x>=2 AND a.x<=5 AND b.x=5 AND a.y>=3 AND a.y<=7 AND a.z = 8";
        System.out.println("query "+query);
        sqlQuery2 = SQLQueryParser.parse(query, new NodeHashValues());
        qc.setDemandedQuery(sqlQuery2);

        result = qc.containQuery();
        System.out.println("\n!!!!!result is "+result+"\n\n");
        qc.clear();
        //Telos 222222222222222222222222222222222222222222


        query = "SELECT a.id AS a_id, b.id AS b_id, a.x AS a_x, b.y AS b_y FROM A a, B b WHERE a.id=b.id AND a.id>=5";
        System.out.println("query "+query);
        sqlQuery1 = SQLQueryParser.parse(query, new NodeHashValues());
        qc.setCachedQuery(sqlQuery1, "myTable");

        query = "SELECT a.id AS a_id, b.id AS b_id, c.id AS c_id FROM A a, B b, C c " +
                "WHERE a.x>=1 AND b.y=5 AND a.id>=6 AND a.id=b.id AND a.id=c.id AND c.k <=9 and c.k > 2";
        System.out.println("query "+query);
        sqlQuery2 = SQLQueryParser.parse(query, new NodeHashValues());
        qc.setDemandedQuery(sqlQuery2);

        result = qc.containQuery();
        System.out.println("\n!!!!!result is "+result+"\n\n");
        qc.clear();
        //Telos 33333333333333333333333333333333333333333333


        query = "SELECT a.id AS a_id, b.id AS b_id, a.x AS a_x, b.y AS b_y FROM A a, B b WHERE a.id=b.id AND a.id>=5";
        System.out.println("query "+query);
        sqlQuery1 = SQLQueryParser.parse(query, new NodeHashValues());
        qc.setCachedQuery(sqlQuery1, "myTable");

        query = "SELECT a.id AS a_id, b.id AS b_id, c.id AS c_id FROM A a, B b, C c, D d " +
                "WHERE a.x>=1 AND b.y=5 AND a.id>=6 AND a.id=b.id AND a.id=c.id AND c.k <=9 and c.k > 2 " +
                "and d.id=c.id";
        System.out.println("query "+query);
        sqlQuery2 = SQLQueryParser.parse(query, new NodeHashValues());
        qc.setDemandedQuery(sqlQuery2);

        result = qc.containQuery();
        System.out.println("\n!!!!!result is "+result+"\n\n");
        qc.clear();
        //Telos 44444444444444444444444444444444444444444444


        //elegxos tou strict matching sta filtra
        query = "SELECT * FROM A a WHERE a.id >= 2";
        System.out.println("query "+query);
        sqlQuery1 = SQLQueryParser.parse(query, new NodeHashValues());
        qc.setCachedQuery(sqlQuery1, "myTable");

        query = "SELECT * FROM A a WHERE a.id>=4 AND a.id>=3";
        System.out.println("query "+query);
        sqlQuery2 = SQLQueryParser.parse(query, new NodeHashValues());
        qc.setDemandedQuery(sqlQuery2);

        result = qc.containQuery();
        System.out.println("\n!!!!!result is "+result+"\n\n");
        qc.clear();
        //Telos 555555555555555555555555555555555555555555555555


        //elegxos ths aporipsis pediwn sto select tou demand pou dn uparxoun sto cached select
        query = "SELECT a.id as a_id FROM A a WHERE a.id >= 2";
        System.out.println("query "+query);
        sqlQuery1 = SQLQueryParser.parse(query, new NodeHashValues());
        qc.setCachedQuery(sqlQuery1, "myTable");

        query = "SELECT a.id as a_id, a.x as a_x FROM A a WHERE a.id>=4";
        System.out.println("query "+query);
        sqlQuery2 = SQLQueryParser.parse(query, new NodeHashValues());
        qc.setDemandedQuery(sqlQuery2);

        result = qc.containQuery();
        System.out.println("\n!!!!!result is "+result+"\n\n");
        qc.clear();
        //Telos 66666666666666666666666666666666666666666666666


        //elegxos ths aporipsis pediwn sto where tou demand pou dn uparxoun sto cached select
        query = "SELECT a.id as a_id FROM A a WHERE a.id >= 2";
        System.out.println("query "+query);
        sqlQuery1 = SQLQueryParser.parse(query, new NodeHashValues());
        qc.setCachedQuery(sqlQuery1, "myTable");

        query = "SELECT a.id as a_id FROM A a WHERE a.id>=4 and a.x>=3";
        System.out.println("query "+query);
        sqlQuery2 = SQLQueryParser.parse(query, new NodeHashValues());
        qc.setDemandedQuery(sqlQuery2);

        result = qc.containQuery();
        System.out.println("\n!!!!!result is "+result+"\n\n");
        qc.clear();
        //Telos 777777777777777777777777777777777777777777777777


        //elegxos ths aporipsis pediwn sto where tou demand pou dn uparxoun sto cached select
        query = "SELECT a.id as a_id FROM A a WHERE a.id >= 2";
        System.out.println("query "+query);
        sqlQuery1 = SQLQueryParser.parse(query, new NodeHashValues());
        qc.setCachedQuery(sqlQuery1, "myTable");

        query = "SELECT a.id as a_id FROM A a, B b WHERE a.id>=4 and a.x=b.x";
        System.out.println("query "+query);
        sqlQuery2 = SQLQueryParser.parse(query, new NodeHashValues());
        qc.setDemandedQuery(sqlQuery2);

        result = qc.containQuery();
        System.out.println("\n!!!!!result is "+result+"\n\n");
        qc.clear();
        //Telos 888888888888888888888888888888888888888888888888


        //elegxos tou string compare
        query = "SELECT a.name as a_name FROM A a WHERE a.name >= 'a'";
        System.out.println("query "+query);
        sqlQuery1 = SQLQueryParser.parse(query, new NodeHashValues());
        qc.setCachedQuery(sqlQuery1, "myTable");

        query = "SELECT a.name as a_name FROM A a WHERE a.name >= 'b'";
        System.out.println("query "+query);
        sqlQuery2 = SQLQueryParser.parse(query, new NodeHashValues());
        qc.setDemandedQuery(sqlQuery2);

        result = qc.containQuery();
        System.out.println("\n!!!!!result is "+result+"\n\n");
        qc.clear();
        //Telos 999999999999999999999999999999999999999999999999999


        //elegxos tou string compare
        query = "SELECT a.name as a_name FROM A a WHERE a.name = 'b'";
        System.out.println("query "+query);
        sqlQuery1 = SQLQueryParser.parse(query, new NodeHashValues());
        qc.setCachedQuery(sqlQuery1, "myTable");

        query = "SELECT a.name as a_name FROM A a WHERE a.name = 'b'";
        System.out.println("query "+query);
        sqlQuery2 = SQLQueryParser.parse(query, new NodeHashValues());
        qc.setDemandedQuery(sqlQuery2);

        result = qc.containQuery();
        System.out.println("\n!!!!!result is "+result+"\n\n");
        qc.clear();
        //Telos 101010101010101010101010101010101010101010101010101010

        //elegxos tou date compare
        query = "SELECT a.date as a_date FROM A a WHERE a.date >= '03-Aug-2006 18:55:30'";
        System.out.println("query "+query);
        sqlQuery1 = SQLQueryParser.parse(query, new NodeHashValues());
        qc.setCachedQuery(sqlQuery1, "myTable");

        query = "SELECT a.date as a_date FROM A a WHERE a.date >= '03-Aug-2006 18:55:30'";
        System.out.println("query "+query);
        sqlQuery2 = SQLQueryParser.parse(query, new NodeHashValues());
        qc.setDemandedQuery(sqlQuery2);

        result = qc.containQuery();
        System.out.println("\n!!!!!result is "+result+"\n\n");
        qc.clear();
        //Telos 111111111111111111111111111111111111111111111111111111


        //elegxos tou date compare
        query = "SELECT a.date as a_date FROM A a WHERE a.date >= '03-Aug-2006 18:55:30'";
        System.out.println("query "+query);
        sqlQuery1 = SQLQueryParser.parse(query, new NodeHashValues());
        qc.setCachedQuery(sqlQuery1, "myTable");

        query = "SELECT a.date as a_date FROM A a WHERE a.date >= '02-Aug-2016 18:55:30'";
        System.out.println("query "+query);
        sqlQuery2 = SQLQueryParser.parse(query, new NodeHashValues());
        qc.setDemandedQuery(sqlQuery2);

        result = qc.containQuery();
        System.out.println("\n!!!!!result is "+result+"\n\n");
        qc.clear();
        //Telos 12121212121212121212121212121212121212121212121212121212

        //elegxos tou date compare
        query = "SELECT a.date as a_date FROM A a WHERE a.date >= '03-Aug-2006 18:55:30'";
        System.out.println("query "+query);
        sqlQuery1 = SQLQueryParser.parse(query, new NodeHashValues());
        qc.setCachedQuery(sqlQuery1, "myTable");

        query = "SELECT a.date as a_date FROM A a WHERE a.date >= '02-Aug-2006 18:55:30'";
        System.out.println("query "+query);
        sqlQuery2 = SQLQueryParser.parse(query, new NodeHashValues());
        qc.setDemandedQuery(sqlQuery2);

        result = qc.containQuery();
        System.out.println("\n!!!!!result is "+result+"\n\n");
        qc.clear();
        //Telos 1313131313131313131313131313131313131313131313131313131313


        query = "SELECT a.date as a_date FROM A a WHERE a.date >= '03-Aug-2006 18:55:30'";
        System.out.println("query "+query);
        sqlQuery1 = SQLQueryParser.parse(query, new NodeHashValues());
        qc.setCachedQuery(sqlQuery1, "myTable");

        query = "SELECT a.date as a_date FROM A a WHERE a.date >= '04-Aug-2006 18:55:30'";
        System.out.println("query "+query);
        sqlQuery2 = SQLQueryParser.parse(query, new NodeHashValues());
        qc.setDemandedQuery(sqlQuery2);

        result = qc.containQuery();
        System.out.println("\n!!!!!result is "+result+"\n\n");
        qc.clear();

    }

}

package madgik.exareme.master.importer;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;

import madgik.exareme.master.dbmanager.DBManager;
import madgik.exareme.master.queryProcessor.analyzer.fanalyzer.SPARQLAnalyzer;
import madgik.exareme.master.queryProcessor.analyzer.stat.StatUtils;
import madgik.exareme.master.queryProcessor.decomposer.dag.Node;
import madgik.exareme.master.queryProcessor.decomposer.dag.NodeHashValues;
import madgik.exareme.master.queryProcessor.decomposer.federation.Memo;
import madgik.exareme.master.queryProcessor.decomposer.federation.SinglePlan;
import madgik.exareme.master.queryProcessor.decomposer.federation.SinlgePlanDFLGenerator;
import madgik.exareme.master.queryProcessor.decomposer.query.Column;
import madgik.exareme.master.queryProcessor.decomposer.query.NonUnaryWhereCondition;
import madgik.exareme.master.queryProcessor.decomposer.query.Output;
import madgik.exareme.master.queryProcessor.decomposer.query.SQLQuery;
import madgik.exareme.master.queryProcessor.decomposer.query.Table;
import madgik.exareme.master.queryProcessor.estimator.NodeSelectivityEstimator;
import madgik.exareme.master.queryProcessor.estimator.db.Schema;
import madgik.exareme.master.queryProcessor.sparql.DagCreator;
import madgik.exareme.master.queryProcessor.sparql.DagExpander;
import madgik.exareme.master.queryProcessor.sparql.IdFetcher;

public class Importer {

	public static void main(String[] args) throws IOException, SQLException {
		boolean importData = false;
		boolean analyze=false;
		boolean run=true;
		
		DBManager m=new DBManager();
		if(importData){
		InputStream s=readFile("/media/dimitris/T/lubm100/University0-99-clean2.nt");
		RDFParser rdfParser = Rio.createParser(RDFFormat.TURTLE);
		ImportHandler h=new ImportHandler(m.getConnection("/media/dimitris/T/test2/"), 4);
		rdfParser.setRDFHandler(h);
		try {
			   rdfParser.parse(s, "http://me.org/");
			}
			catch (IOException e) {
			  // handle IO problems (e.g. the file could not be read)
			}
			catch (RDFParseException e) {
			  // handle unrecoverable parse error
			}
			catch (RDFHandlerException e) {
			  // handle a problem encountered by the RDFHandler
			}
		}
		if(analyze){
			SPARQLAnalyzer a=new SPARQLAnalyzer(4, m.getConnection("/media/dimitris/T/test2/"));
			try {
					
				Schema stats=a.analyze();
				StatUtils.addSchemaToFile("/media/dimitris/T/test2/" + "histograms.json", stats);
				
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
		if(run){
			try {
				String prefixes="PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX ub:<http://swat.cse.lehigh.edu/onto/univ-bench.owl#> ";
				String q="SELECT ?y ?b ?z  WHERE { ?y ?b ?z . ?a ?v ?z }";
				String q2="SELECT ?x ?y ?z WHERE {  ?y rdf:type ub:FullProfessor . ?y ub:teacherOf ?z .  ?z rdf:type ub:Course . ?x ub:advisor ?y . ?x rdf:type ub:UndergraduateStudent . ?x ub:takesCourse ?z }";
				String q3="SELECT ?x ?y ?z WHERE {  ?y ub:teacherOf ?z .  ?z rdf:type ub:Course . ?x ub:advisor ?y .  ?x ub:takesCourse ?z }";
				
				NodeSelectivityEstimator nse=new NodeSelectivityEstimator("/home/dimitris/sparqlhist/" + "histograms.json");
				NodeHashValues hashes=new NodeHashValues();
				hashes.setSelectivityEstimator(nse);
				IdFetcher fetcher=new IdFetcher(m.getConnection("/home/dimitris/sparqlhist/"));
				DagCreator creator=new DagCreator(prefixes+q2, 4, hashes, fetcher);
				Node root=creator.getRootNode();
				//System.out.println(root.count(0));
				long start=System.currentTimeMillis();
				//System.out.println(root.dotPrint(new HashSet<Node>()));
				DagExpander expander=new DagExpander(root, hashes);
				expander.expand();
				//System.out.println(root.dotPrint(new HashSet<Node>()));
				Memo memo=new Memo();
				SinglePlan plan = expander.getBestPlanCentralized(root, Double.MAX_VALUE, memo);
				System.out.println(System.currentTimeMillis()-start);
				SinlgePlanDFLGenerator dsql = new SinlgePlanDFLGenerator(root, 1, memo);
				//dsql.setN2a(n2a);
				SQLQuery result=dsql.generate().get(0);
				
				//TODO add virtual table when needed
				for(int i=1;i<result.getInputTables().size();i++){
					Table t=result.getInputTables().get(i);
					t.setName("wrapper"+t.getName());
				}
				
				
				//add dictionary lookups
				int out=1;
				Map<Column, Column> toChange=new HashMap<Column, Column>();
				for(Column outCol:result.getAllOutputColumns()){
					String alias="d"+out;
					out++;
					result.addInputTable(new Table("dictionary", alias));
					NonUnaryWhereCondition dictJoin=new NonUnaryWhereCondition(outCol.clone(), new Column(alias, "id"), "=");
					result.addBinaryWhereCondition(dictJoin);
					toChange.put(outCol.clone(), new Column(alias, "uri"));
				}
				for(Output o:result.getOutputs()){
					for(Column c:toChange.keySet()){
						o.getObject().changeColumn(c, toChange.get(c));
					}
				}
				
				
				
				System.out.println( result.toDistSQL() );
				//System.out.println(root.count(0));
				System.out.println("OK");
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}
	
	public static InputStream readFile(String f) 
			  throws IOException {
			    File initialFile = new File(f);
			    return FileUtils.openInputStream(initialFile);
			}

}

package madgik.exareme.master.importer;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;

import org.apache.commons.io.FileUtils;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;

import madgik.exareme.master.dbmanager.DBManager;
import madgik.exareme.master.queryProcessor.analyzer.fanalyzer.SPARQLAnalyzer;

public class Importer {

	public static void main(String[] args) throws IOException, SQLException {
		boolean importData = false;
		boolean analyze=true;
		
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
				a.analyze();
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

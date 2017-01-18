package madgik.exareme.master.queryProcessor.decomposer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import madgik.exareme.master.queryProcessor.decomposer.dag.NodeHashValues;
import madgik.exareme.master.queryProcessor.decomposer.federation.DBInfoReaderDB;
import madgik.exareme.master.queryProcessor.decomposer.federation.NamesToAliases;
import madgik.exareme.master.queryProcessor.decomposer.federation.QueryDecomposer;
import madgik.exareme.master.queryProcessor.decomposer.query.SQLQuery;
import madgik.exareme.master.queryProcessor.decomposer.query.SQLQueryParser;
import madgik.exareme.master.queryProcessor.estimator.NodeSelectivityEstimator;

public class DemoDAG {

	
	
	public static void main(String[] args) throws Exception {
		
	
	
		long startt=System.currentTimeMillis();
		getDFLsFromDir("/home/dimitris/Dropbox/npdsql/npdnew100/");
		System.out.println("total time:"+(System.currentTimeMillis()-startt));
	
		

	}
	
	private static void getDFLsFromDir(String dir) throws Exception{
		for(String file:readFilesFromDir(dir)){
			getDFLFromFile(file);
		}
	}
	
	private static void getDFLFromFile(String file) throws Exception{
		String q = readFile(file);
		if(q.isEmpty()){
			return;
		}
		NodeHashValues hashes=new NodeHashValues();
		
		NodeSelectivityEstimator nse = null;
		try {
			//nse = new NodeSelectivityEstimator("/home/dimitris/" + "histograms-replaced.json");
			nse = new NodeSelectivityEstimator("/media/dimitris/T/exaremenpd500new/" + "histograms.json");
		} catch (Exception e) {
			
		}
		hashes.setSelectivityEstimator(nse);
		NamesToAliases n2a=new NamesToAliases();
		n2a = DBInfoReaderDB.readAliases("/tmp/");
		Map<String, Set<String>> refCols=new HashMap<String, Set<String>>();
		SQLQuery query = SQLQueryParser.parse(q, hashes, n2a, refCols);
		QueryDecomposer d = new QueryDecomposer(query, "/tmp/", 1, hashes, false);
		d.addRefCols(refCols);
		d.setN2a(n2a);
		StringBuffer sb=new StringBuffer();
		for (SQLQuery s : d.getSubqueries()) {
			sb.append("\n");
			sb.append(s.toDistSQL());
		}
		writeFile(file+".dfl", sb.toString());
	}
	
	private static String readFile(String file) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(file));
		String line = null;
		StringBuilder stringBuilder = new StringBuilder();
		String ls = System.getProperty("line.separator");

		while ((line = reader.readLine()) != null) {
			stringBuilder.append(line);
			stringBuilder.append(ls);
		}
		reader.close();
		return stringBuilder.toString();
	}
	
    private static String[] readFilesFromDir(String string) throws IOException {
    	File folder = new File(string);
    	File[] listOfFiles = folder.listFiles();
    	List<String> files=new ArrayList<String>();
    	    for (int i = 0; i < listOfFiles.length; i++) {
    	      if (listOfFiles[i].isFile()&&listOfFiles[i].getCanonicalPath().endsWith("29.q.sql")) {
    	    	  files.add(listOfFiles[i].getCanonicalPath());
    	      }
    	    }
    	    return files.toArray(new String[files.size()]);
	}

	public static void writeFile(String filename, String string) {
		writeFile(filename, string.getBytes());
	}
	public static void writeFile(String filename, byte[] string) {
		try {
			File file = new File(filename);
			file.getParentFile().mkdirs();
			OutputStream out = new FileOutputStream(file);
			out.write(string);
			out.close();
		} catch (Exception e) {
			System.err.println("Error writing file: " + filename);
			e.printStackTrace();
		}
	}
    
}

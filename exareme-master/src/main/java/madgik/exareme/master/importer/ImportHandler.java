package madgik.exareme.master.importer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.URI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;


public class ImportHandler extends AbstractRDFHandler {
	
	private long id=0l;
	private int partitions;
	private int propertyId=0;
	private Connection con;
	private java.sql.Statement stmt;
	private PreparedStatement getId;
	private PreparedStatement insertId;
	private PreparedStatement insertProperty;
	private Map<String, String> propertyTables;
	
	
	
	public ImportHandler(Connection c, int partNo) throws SQLException {
		this.partitions=partNo;
		this.con = c;
		this.stmt=con.createStatement();
		propertyTables=new HashMap<String, String>();
		createDictionary();
		this.getId=con.prepareStatement("select id from dictionary where uri=?");
		this.insertId=con.prepareStatement("insert into dictionary values(?, ?)");
		this.insertProperty=con.prepareStatement("insert into properties values(?, ?)");
	}



	private void createDictionary() throws SQLException {
		stmt.execute("create table dictionary(id INTEGER PRIMARY KEY, uri TEXT)");
		stmt.execute("create unique index uriindex on dictionary(uri)");	
		stmt.execute("create table properties(id INTEGER PRIMARY KEY, uri TEXT)");
		stmt.execute("create unique index propertyindex on properties(uri)");
	}



	@Override
	  public void handleStatement(Statement st) {
		Resource subj = st.getSubject();
		try{
		
		String predicate=st.getPredicate().stringValue();
		String propTable="";
		if(propertyTables.containsKey(predicate)){
			propTable=propertyTables.get(predicate);
		}
		else{
			insertProperty.clearBatch();
			insertProperty.setInt(1, propertyId);
			insertProperty.setString(2, predicate);
			//insertProperty.addBatch();
			insertProperty.executeUpdate();
			
			propertyTables.put(predicate, "prop"+propertyId);
			createPropertyTables();
			propTable="prop"+propertyId;
			propertyId++;
		}
		
			
		if (subj instanceof URI) {
			URI uri=(URI)subj;
			String uriString=uri.stringValue();
			long objectLong=0L;
				long subjectLong=getIdForUri(uriString);
			Value v=st.getObject();
			if (v instanceof URI) {
				 objectLong=getIdForUri(v.stringValue());
			}
			else if (v instanceof BNode){
				throw new SQLException("blank nodes currently not supported");
			}
			else if(v instanceof Literal){
				objectLong=getIdForUri(v.stringValue());
			}
			long mod=subjectLong%this.partitions;
			long modInv=objectLong%this.partitions;
			stmt.execute("insert or ignore into "+propTable+"_"+mod+" values("+subjectLong+", "+objectLong+")");
			stmt.execute("insert or ignore into inv"+propTable+"_"+modInv+" values("+objectLong+", "+subjectLong+")");
		} else if (subj instanceof BNode) {
			throw new SQLException("blank nodes currently not supported");
		} 
		
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	  }



	private void createPropertyTables() throws SQLException {
		for(int i=0;i<this.partitions;i++){
			stmt.execute("create table prop"+this.propertyId+"_"+i+" (s INTEGER, o INTEGER, primary key(s, o)) without rowid");
			stmt.execute("create table invprop"+this.propertyId+"_"+i+" (o INTEGER, s INTEGER, primary key(o, s)) without rowid");
		}
	}



	private long getIdForUri(String uriString) throws SQLException {
		long result=this.id;
		getId.setString(1, uriString);
		ResultSet rs=getId.executeQuery();
		if(rs.next()){
			result=rs.getLong(1);
		}
		else{
			insertId.setLong(1, id);
			insertId.setString(2, uriString);
			insertId.execute();
			id++;
		}
		return result;
	}

}

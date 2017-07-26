package madgik.exareme.master.importer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.URI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;

public class ImportHandler extends AbstractRDFHandler {

	//private long id = 1l;
	// private int partitions;
	private int propertyId = 0;
	private Connection con;
	private java.sql.Statement stmt;
	private PreparedStatement getId;
	//private PreparedStatement insertId;
	private PreparedStatement insertProperty;
	private Map<String, Integer> propertyTables;
	//private Map<String, Long> idCache;
	private List<PreparedStatement> inserts;
	private final int cacheSize = 30000;
	private int currentCache = 0;

	public ImportHandler(Connection c, int partNo) throws SQLException {
		// this.partitions = partNo;
		this.con = c;
		this.stmt = con.createStatement();
		propertyTables = new HashMap<String, Integer>();
		createDictionary();
		this.getId = con.prepareStatement("select id from ddd where uri=?");
		//this.insertId = con.prepareStatement("insert into dictionary values(?, ?)");
		this.insertProperty = con.prepareStatement("insert into properties values(?, ?)");
		//idCache = new HashMap<String, Long>(200000);
		inserts = new ArrayList<PreparedStatement>();
	}

	private void createDictionary() throws SQLException {
		//stmt.excute("BEGIN");
		stmt.execute("create virtual table ddd using trie()");
		//stmt.execute("create table dictionary(id INTEGER PRIMARY KEY, uri TEXT)");
		//stmt.execute("create unique index uriindex on dictionary(uri)");
		stmt.execute("create table properties(id INTEGER PRIMARY KEY, uri TEXT)");
		stmt.execute("create unique index propertyindex on properties(uri)");
	}

	@Override
	public void handleStatement(Statement st) {
		Resource subj = st.getSubject();
		try {

			String predicate = st.getPredicate().stringValue();
			int propTable = -1;
			if (propertyTables.containsKey(predicate)) {
				propTable = propertyTables.get(predicate);
			} else {
				insertProperty.clearBatch();
				insertProperty.setInt(1, propertyId);
				insertProperty.setString(2, predicate);
				// insertProperty.addBatch();
				insertProperty.executeUpdate();

				propertyTables.put(predicate, propertyId);
				createPropertyTables();
				propTable = propertyId;
				propertyId++;

				String propName = "prop" + propTable;

				PreparedStatement ps = con.prepareStatement("insert or ignore into " + propName + " values(?, ?) ");
				inserts.add(propTable * 2, ps);
				propName = "inv" + propName;
				ps = con.prepareStatement("insert or ignore into " + propName + " values(?, ?) ");
				inserts.add((propTable * 2) + 1, ps);

			}

			if (subj instanceof URI) {
				URI uri = (URI) subj;
				String uriString = uri.stringValue();
				long objectLong = 0L;
				long subjectLong = getIdForUri(uriString);
				Value v = st.getObject();
				if (v instanceof URI) {
					objectLong = getIdForUri(v.stringValue());
				} else if (v instanceof BNode) {
					throw new SQLException("blank nodes currently not supported");
				} else if (v instanceof Literal) {
					objectLong = getIdForUri(v.stringValue());
				}
				int index = propTable * 2;
				/*PreparedStatement s = inserts.get(index);
				s.setLong(1, subjectLong);
				s.setLong(2, objectLong);
				s.addBatch();
				PreparedStatement o = inserts.get(index + 1);
				o.setLong(2, subjectLong);
				o.setLong(1, objectLong);
				o.addBatch();
				currentCache++;
				if (currentCache == cacheSize) {
					executeBatch();
				}*/

			} else if (subj instanceof BNode) {
				throw new SQLException("blank nodes currently not supported");
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void executeBatch() throws SQLException {
		for (PreparedStatement ps : inserts) {
			ps.executeBatch();
		}
		currentCache = 0;
		this.con.commit();
	}

	private void createPropertyTables() throws SQLException {
		stmt.execute(
				"create table prop" + this.propertyId + " (s INTEGER, o INTEGER) ");
		stmt.execute(
				"create table invprop" + this.propertyId + " (o INTEGER, s INTEGER)");

	}

	private long getIdForUri(String uriString) throws SQLException {
		long result=-1;
		/*getId.setString(1, uriString);
		ResultSet rs = getId.executeQuery();
		if (rs.next()) {
			result = rs.getLong(1);
		} 
		rs.close();*/
		return result;
	}

	@Override
	public void endRDF() throws RDFHandlerException {
		System.out.println("ending...");
		try {
			
			stmt.close();
			getId.close();
			insertProperty.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}

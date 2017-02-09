package madgik.exareme.master.queryProcessor.sparql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class IdFetcher {
	
	private Connection con;
	private PreparedStatement getId;
	private PreparedStatement getProperty;

	public IdFetcher(Connection con) throws SQLException {
		super();
		this.con = con;
		this.getId=con.prepareStatement("select id from dictionary where uri=?");
		this.getProperty=con.prepareStatement("select id from properties where uri=?");
	}
	
	public long getIdForUri(String uriString) throws SQLException {
		//getId.clearBatch();
		getId.setString(1, uriString);
		ResultSet rs=getId.executeQuery();
		if(rs.next()){
			long res=rs.getLong(1);
			rs.close();
			return res;
		}
		else{
			rs.close();
			return -1L;
		}
	}
	
	public long getIdForProperty(String uriString) throws SQLException {
		getProperty.setString(1, uriString);
		ResultSet rs=getProperty.executeQuery();
		if(rs.next()){
			long res=rs.getLong(1);
			rs.close();
			return res;
		}
		else{
			rs.close();
			throw new SQLException("property "+uriString+" does not exist in RDF graph");
		}
	}

}

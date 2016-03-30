package madgik.exareme.master.queryProcessor.decomposer;

import java.sql.Statement;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class IndexCreator {

	public static void main(String[] args) throws IOException, SQLException {
		// TODO Auto-generated method stub
		boolean declaredIndices=false;
		String dir="/media/dimitris/T/exaremenpd100/";
		String declared="/home/dimitris/ontopv1/npd-benchmark/data/db_creation_scripts/postgres/postgresindexes.sql";
		String automatic="/home/dimitris/ontopv1/npd-benchmark/data/db_creation_scripts/postgres/postgresAutomaticIndexes.sql";
		String file=automatic;
		if(declaredIndices){
			file=declared;
		}
				for(String command:readFile(file)){
			String tablename=command.split("\"")[3];
			Connection sqliteConnection = DriverManager.getConnection("jdbc:sqlite:"
					+ dir + tablename + ".0.db");
			Statement st=sqliteConnection.createStatement();
			st.execute(command);
			st.close();
			sqliteConnection.close();
		}
	}
	
	private static List<String> readFile(String file) throws IOException {
		List<String> result=new ArrayList<String>();
		BufferedReader reader = new BufferedReader(new FileReader(file));
		String line = null;
		StringBuilder stringBuilder = new StringBuilder();
		String ls = System.getProperty("line.separator");

		while ((line = reader.readLine()) != null) {
			if(line.equals(";")){
				result.add(stringBuilder.toString());
				stringBuilder = new StringBuilder();
			}
			else{
				stringBuilder.append(line);
				stringBuilder.append(ls);
			}
			
		}
		reader.close();
		return result;
	}
}

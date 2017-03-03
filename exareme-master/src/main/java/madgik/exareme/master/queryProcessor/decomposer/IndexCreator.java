package madgik.exareme.master.queryProcessor.decomposer;

import java.sql.Statement;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class IndexCreator {

	public static void main(String[] args) throws IOException, SQLException {
		// TODO Auto-generated method stub
		boolean declaredIndices = true;
		String dir = "/media/dimitris/T/exaremenpd1500/";
		String declared = "/home/dimitris/ontopv1/npd-benchmark/data/db_creation_scripts/postgres/postgresindexes.sql";
		String automatic = "/home/dimitris/ontopv1/npd-benchmark/data/db_creation_scripts/postgres/postgresAutomaticIndexes.sql";
		String file = automatic;
		if (declaredIndices) {
			file = declared;
		}
		for (String command : readFile(file)) {
			String tablename = command.split("\"")[3];
			Connection sqliteConnection = DriverManager.getConnection("jdbc:sqlite:" + dir + tablename + ".0.db");
			Statement st = sqliteConnection.createStatement();
			st.execute(command);
			st.close();
			sqliteConnection.close();
		}

		boolean createTestDb = false;
		if (createTestDb) {
			Connection sqliteConnection = DriverManager.getConnection("jdbc:sqlite:/media/dimitris/T/test/rdf" + ".db");
			sqliteConnection.setAutoCommit(false);
			PreparedStatement st = sqliteConnection.prepareStatement("insert OR IGNORE into S values(?, ?)");
			Random random = new Random();
			for (int i = 0; i < 10000000; i++) {
				int a = random.nextInt();
				int b = random.nextInt(1000);
				st.setInt(1, a);
				st.setInt(2, b);
				st.execute();
			}

			sqliteConnection.commit();
			st.close();
			sqliteConnection.close();
		}
	}

	private static List<String> readFile(String file) throws IOException {
		List<String> result = new ArrayList<String>();
		BufferedReader reader = new BufferedReader(new FileReader(file));
		String line = null;
		StringBuilder stringBuilder = new StringBuilder();
		String ls = System.getProperty("line.separator");

		while ((line = reader.readLine()) != null) {
			if (line.equals(";")) {
				result.add(stringBuilder.toString());
				stringBuilder = new StringBuilder();
			} else {
				stringBuilder.append(line);
				stringBuilder.append(ls);
			}

		}
		reader.close();
		return result;
	}
}

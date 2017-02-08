package madgik.exareme.master.queryProcessor.decomposer.federation;


import madgik.exareme.master.queryProcessor.decomposer.DecomposerUtils;
import madgik.exareme.master.queryProcessor.decomposer.query.SQLQuery;
import madgik.exareme.master.queryProcessor.decomposer.query.Table;


import org.apache.log4j.Logger;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class DataImporter implements Runnable {
	private SQLQuery s;
	private String dbPath;
	private DB db;
	private boolean addToRegistry;
	private String fedSQLTrue;
	private String fedSQLFalse;
	private String error;
	Map<String, String> correspondingOutputs;
	
	private static final Logger log = Logger.getLogger(DataImporter.class);

	public DataImporter(SQLQuery q, String db, DB dbinfo) {
		this.db=dbinfo;
		this.s = q;
		this.dbPath = db;
		this.addToRegistry=false;
        correspondingOutputs=new HashMap<String, String>();
		if (this.db.getDriver().contains("OracleDriver")) {
			correspondingOutputs=s.renameOracleOutputs();
		}
		fedSQLTrue=s.getExecutionStringInFederatedSource(true);
		fedSQLFalse=s.getExecutionStringInFederatedSource(false);
		
	}

	@Override
	public void run() {
		int columnsNumber=0;
		//DB db = DBInfoReaderDB.dbInfo.getDBForMadis(s.getMadisFunctionString());
		StringBuilder createTableSQL = new StringBuilder();
		if (db == null) {
			log.error("Could not import Data. DB not found:"
					+ s.getMadisFunctionString());
			return;
		}
		String driverClass = db.getDriver();
		try {
			Class.forName(driverClass);
			Class.forName("org.sqlite.JDBC");
		} catch (ClassNotFoundException e1) {
			log.error("Could not import Data. Driver not found:" + driverClass);
			return;
		}

		String conString = db.getURL();
		
		
		

		//fedSQL = s.getExecutionStringInFederatedSource(true);
		log.debug("importing:" + fedSQLTrue);
		Connection connection = null;
		Statement statement = null;
		ResultSet resultSet = null;
		long start = -1;
		int count = 0;
		Connection sqliteConnection = null;
		PreparedStatement sqliteStatement = null;
		String importString="import/";
		String part=".db";
		if(addToRegistry){
			importString="";
			part=".0.db";
		}

		try {
			
			sqliteConnection = DriverManager.getConnection("jdbc:sqlite:"
					+ dbPath + importString + s.getTemporaryTableName() + part);
			// statement.setQueryTimeout(30);
			sqliteConnection.setAutoCommit(false);
			connection = DriverManager.getConnection(conString, db.getUser(),
					db.getPass());
			int fetch = 100;
			if (db.getDriver().contains("OracleDriver")) {
				fetch = DecomposerUtils.FETCH_SIZE_ORACLE;
			} else if (db.getDriver().contains("postgresql")) {
				fetch = DecomposerUtils.FETCH_SIZE_POSTGRES;
				connection.setAutoCommit(false);
			} else if (db.getDriver().contains("mysql")) {
				//hint to mysql jdbc to "stream" results
				fetch = Integer.MIN_VALUE;
				connection.setAutoCommit(false);
			}

			String sql = "insert into " + s.getTemporaryTableName()
					+ " values (";

			
			createTableSQL.append("CREATE TABLE ");
			createTableSQL.append(s.getTemporaryTableName());
			createTableSQL.append("( ");

			statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY,
					ResultSet.CONCUR_READ_ONLY);
			statement.setFetchSize(fetch);
			start = System.currentTimeMillis();

			if (db.getDriver().contains("postgresql")
					&& DecomposerUtils.USE_POSTGRES_COPY) {
				SQLiteWriter swriter=new SQLiteWriter(sqliteConnection, DecomposerUtils.NO_OF_RECORDS, fedSQLFalse, statement, sql, createTableSQL);
				CopyManager copyManager = new CopyManager((BaseConnection) connection);
	            count=(int)copyManager.copyOut("COPY ("+fedSQLTrue+") TO STDOUT WITH DELIMITER '#'", swriter);
	            swriter.close();
			} else {
				StringBuffer primaryKeySQL=new StringBuffer();
				primaryKeySQL.append(", PRIMARY KEY(");
				resultSet = statement.executeQuery(fedSQLTrue);

				ResultSetMetaData rsmd = resultSet.getMetaData();
				columnsNumber = rsmd.getColumnCount();
				String comma = "";
				String questionmark = "?";
				for (int i = 1; i <= columnsNumber; i++) {
					sql += questionmark;
					questionmark = ",?";
					String l = rsmd.getColumnLabel(i);
					
					if(correspondingOutputs.containsKey(l.toUpperCase())){
						l=correspondingOutputs.get(l.toUpperCase());						
					}
					int type = rsmd.getColumnType(i);
					String coltype = "";
					if (JdbcDatatypesToSQLite.intList.contains(type)) {
						coltype = "INTEGER";
					} else if (JdbcDatatypesToSQLite.numericList.contains(type)) {
						coltype = "NUMERIC";
					} else if (JdbcDatatypesToSQLite.realList.contains(type)) {
						coltype = "REAL";
					} else if (JdbcDatatypesToSQLite.textList.contains(type)) {
						coltype = "TEXT";
					} else if (JdbcDatatypesToSQLite.BLOB == type) {
						coltype = "BLOB";
					}
					primaryKeySQL.append(comma);
					primaryKeySQL.append(l);
					createTableSQL.append(comma);
					createTableSQL.append(l);
					createTableSQL.append(" ");
					createTableSQL.append(coltype);
					comma = ",";
				}
				sql += ")";
				if(DecomposerUtils.USE_ROWID&&s.isOutputColumnsDinstict()&&rsmd.getColumnCount()==1){
					primaryKeySQL.append(") ) without rowid");
					createTableSQL.append(primaryKeySQL);
				}
				else{
					createTableSQL.append(")");
				}
				
				Statement creatSt = sqliteConnection.createStatement();
				log.debug("dropping table if exists");
				creatSt.execute("drop table if exists "+s.getTemporaryTableName());
				log.debug("executing:" +createTableSQL);
				creatSt.execute(createTableSQL.toString());
				creatSt.close();
				sqliteStatement = sqliteConnection.prepareStatement(sql);
				final int batchSize = DecomposerUtils.NO_OF_RECORDS;
				while (resultSet.next()) {

					

					for (int i = 1; i <= columnsNumber; i++) {
						Object ob=resultSet.getObject(i);
						if(ob instanceof Date){
							sqliteStatement.setObject(i, ob.toString());
						}
						else{
						sqliteStatement.setObject(i, ob);
						}
					}

					sqliteStatement.addBatch();

					if (++count % batchSize == 0) {
						sqliteStatement.executeBatch();
					}
				}
				sqliteStatement.executeBatch(); // insert remaining records
				/*if(DecomposerUtils.USE_ROWID&&!s.isOutputColumnsDinstict()&&rsmd.getColumnCount()==1){
					String index="create index "+rsmd.getColumnName(1)+"index"+" on "+s.getTemporaryTableName()+"("+
							rsmd.getColumnLabel(1)+")";
					log.debug(index);
					sqliteStatement.execute(index);
				}*/
				sqliteStatement.close();
				resultSet.close();
			}
			if(DecomposerUtils.USE_ROWID&&!s.isOutputColumnsDinstict()&&s.getOutputs().size()==1){
				String index="create index "+s.getOutputAliases().get(0)+"index"+" on "+s.getTemporaryTableName()+"("+
						s.getOutputAliases().get(0)+")";
				log.debug(index);
				Statement creatindex = sqliteConnection.createStatement();
				creatindex.execute(index);
				creatindex.close();
			}
			
			sqliteConnection.commit();
			
			sqliteConnection.close();
			
			statement.close();
			connection.close();

		} catch (Exception e) {
			e.printStackTrace();
			log.error("Could not import data from endpoint\n" + e.getMessage() +
					" from query:"+fedSQLTrue);
			error="Could not import data from endpoint\n" + e.getMessage() +
					" from query:"+fedSQLTrue;
			return;
		}

		log.debug(count + " rows were imported in "
				+ (System.currentTimeMillis() - start) + "msec from query: "+ fedSQLTrue);
		StringBuilder madis = new StringBuilder();
		madis.append("sqlite '");
		madis.append(this.dbPath);
		madis.append("import/");
		madis.append(s.getTemporaryTableName());
		madis.append(".db' ");
		s.setMadisFunctionString(madis.toString());
		s.setSelectAll(true);

		s.removeInfo();
		s.getInputTables()
				.add(new Table(s.getTemporaryTableName(), s
						.getTemporaryTableName()));
/*		if(this.addToRegistry){
			madgik.exareme.common.schema.Table table=new madgik.exareme.common.schema.Table(s.getTemporaryTableName());
			Registry reg = Registry.getInstance(this.dbPath);
			table.setSqlDefinition(createTableSQL.toString());
			//TableInfo ti=new TableInfo(s.getTemporaryTableName());
			//ti.setSqlDefinition(createTableSQL.toString());
			table.setTemp(false);
			table.setSqlQuery(s.toDistSQL());
			table.setSize(count*columnsNumber*4);//to be fixed
			if(s.getHashId()==null){
				log.error("null hash ID for query "+fedSQLTrue);
				table.setHashID(null);
			}
			else{
				table.setHashID(s.getHashId().asBytes());
			}
			PhysicalTable pt=new PhysicalTable(table);
			Partition partition0 = new Partition(s.getTemporaryTableName(), 0);
			//partition0.addLocation("127.0.0.1");
            partition0.addLocation(ArtRegistryLocator.getLocalRmiRegistryEntityName().getIP());
            //partition0.addPartitionColumn("");
			pt.addPartition(partition0);
			reg.addPhysicalTable(pt);
		}*/

	}
	
	public void setAddToRegisrty(boolean b){
		this.addToRegistry=b;
	}

	public void setFedSQL(String fedSQL) {
		this.fedSQLTrue = fedSQL;
		this.fedSQLFalse = fedSQL;
	}
	public String getError(){
		return error;
	}
	

}

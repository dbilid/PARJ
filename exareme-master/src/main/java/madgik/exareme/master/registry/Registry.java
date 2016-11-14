/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package madgik.exareme.master.registry;

import com.google.gson.Gson;
import madgik.exareme.common.schema.Index;
import madgik.exareme.common.schema.Partition;
import madgik.exareme.common.schema.PhysicalTable;
import madgik.exareme.common.schema.Table;
import madgik.exareme.master.client.AdpDBClientProperties;
import madgik.exareme.utils.properties.AdpDBProperties;
import org.apache.log4j.Logger;
import madgik.exareme.utils.association.Triple;

import java.io.File;
import java.io.Serializable;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author Christoforos Svingos
 */
public class Registry {

	private static final Logger log = Logger.getLogger(Registry.class);
	private static List<Registry> registryObjects = new ArrayList<>();
	private Connection regConn = null;
	private String database = null;
	private AdpDBClientProperties properties = null;

	private Registry(String path) {
		new File(path).mkdirs();
		database = path + "/registry.db";
		log.debug("new registry instance...");
		try {
			Class.forName("org.sqlite.JDBC");
			// create a database connection
			regConn = DriverManager.getConnection("jdbc:sqlite:" + database);
			Statement stmt = regConn.createStatement();

			stmt.execute("create table if not exists sql(" + "table_name text, " + "size integer, "
					+ "sql_definition text, " + "sqlQuery text, " + "isTemporary integer, " + "hashID BLOB, "
					+ "pin integer, " + "last_access DATETIME, " + "storage_time DATETIME, " + "num_of_access integer, "
					+ "primary key(table_name));");

			stmt.execute("create table if not exists sqlInfo(" + " table_name text, " + "partition_column text, "
					+ "primary key(table_name));");

			stmt.execute("create table if not exists partition(" + "table_name text, " + "size insqlInfoteger, "
					+ "partition_number integer, " + "location text, " + "partition_column text, "
					+ "primary key(table_name, partition_number, location, partition_column), "
					+ "foreign key(table_name) references sql(table_name));");

			stmt.execute("create table if not exists table_index(" + "index_name text, " + "table_name text, "
					+ "column_name text, " + "partition_number integer, "
					+ "primary key(index_name, partition_number), "
					+ "foreign key(table_name) references sql(table_name));");

			stmt.close();
		} catch (SQLException | ClassNotFoundException e) {
			log.error(e.getMessage(), e);
		}
	}

	public synchronized static Registry getInstance(String path) {
		String db = path + "/registry.db";
		for (Registry registry : registryObjects) {
			if (registry.getDatabase().equals(db))
				return registry;
		}

		Registry registry = new Registry(path);
		registryObjects.add(registry);

		return registry;
	}

	public synchronized static Registry getInstance(AdpDBClientProperties properties) {
		String db = properties.getDatabase() + "/registry.db";
		for (Registry registry : registryObjects) {
			if (registry.getDatabase().equals(db)) {
				registry.setProperties(properties);
				return registry;
			}
		}

		Registry registry = new Registry(properties.getDatabase());
		registry.setProperties(properties);
		registryObjects.add(registry);

		return registry;
	}

	public Registry.Schema getSchema() {
		return new Schema(database, getPhysicalTables());
	}

	public String getDatabase() {
		return database;
	}

	public List<String> getTableDefinitions() {
		List<String> sqlSchemaTables = new ArrayList<String>();

		try (Statement statement = regConn.createStatement()) {
			ResultSet rs = statement.executeQuery("SELECT * FROM sql;");
			while (rs.next()) {

				sqlSchemaTables.add(rs.getString("sql_definition") + ";\n");
			}
		} catch (SQLException ex) {
			log.error(ex.getMessage(), ex);
		}

		return sqlSchemaTables;
	}

	public void addPhysicalTable(PhysicalTable table) {
		Gson gson = new Gson();
		log.info("PhysicalTable Insert Into Registry: " + gson.toJson(table));

		try (PreparedStatement insertSqlStatement = regConn
				.prepareStatement("INSERT INTO sql(table_name, sql_definition, size, isTemporary, "
						+ "sqlQuery, hashID, pin, num_of_access, last_access, storage_time) "
						+ "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

				PreparedStatement insertSqlInfoStatement = regConn
						.prepareStatement("INSERT INTO sqlInfo(table_name, " + "partition_column) VALUES(?, ?)");

				PreparedStatement insertPartitionStatement = regConn
						.prepareStatement("INSERT INTO partition(table_name, " + "location, " + "partition_column, "
								+ "partition_number, size) " + "VALUES(?, ?, ?, ?, ?)");

				PreparedStatement insertIndexStatemenet = regConn
						.prepareStatement("INSERT INTO table_index(index_name, " + "table_name, " + "column_name, "
								+ "partition_number) " + "VALUES(?, ?, ?, ?)")) {
			insertSqlStatement.setString(1, table.getTable().getName());
			insertSqlStatement.setString(2, table.getTable().getSqlDefinition());
			insertSqlStatement.setLong(3, table.getTable().getSize());

			//if (properties != null && properties.isCachedEnable()) {
			//	log.debug("enabled");
				if (table.getTable().isTemp())
					insertSqlStatement.setInt(4, 1);
				else
					insertSqlStatement.setInt(4, 0);
				insertSqlStatement.setString(5, table.getTable().getSqlQuery());
				insertSqlStatement.setBytes(6, table.getTable().getHashID());
				insertSqlStatement.setInt(7, 0);
				insertSqlStatement.setInt(8, 1);
				insertSqlStatement.setString(9,
						madgik.exareme.master.engine.remoteQuery.impl.utility.Date.getCurrentDateTime());
				insertSqlStatement.setString(10,
						madgik.exareme.master.engine.remoteQuery.impl.utility.Date.getCurrentDateTime());
			/*} else {
				log.debug("not enabled");
				insertSqlStatement.setInt(4, 0);
				insertSqlStatement.setString(5, null);
				insertSqlStatement.setBytes(6, null);
				insertSqlStatement.setInt(7, 0);
				insertSqlStatement.setInt(8, 0);
				insertSqlStatement.setString(9, null);
				insertSqlStatement.setString(10, null);

			}*/
			insertSqlStatement.execute();

			for (String partitionColumn : table.getPartitionColumns()) {
				insertSqlInfoStatement.setString(1, table.getTable().getName());
				insertSqlInfoStatement.setString(2, partitionColumn);
				insertSqlInfoStatement.addBatch();
			}
			insertSqlInfoStatement.executeBatch();

			for (Partition partition : table.getPartitions()) {
				for (int i = 0; i < partition.getLocations().size(); ++i) {
					if (partition.getPartitionColumns().isEmpty()) {
						insertPartitionStatement.setString(1, table.getTable().getName());
						insertPartitionStatement.setString(2, partition.getLocations().get(i));
						insertPartitionStatement.setString(3, null);
						insertPartitionStatement.setInt(4, partition.getpNum());
						insertPartitionStatement.setLong(5, partition.getSize());
						insertPartitionStatement.addBatch();
					} else {
						for (int j = 0; j < partition.getPartitionColumns().size(); ++j) {
							insertPartitionStatement.setString(1, table.getTable().getName());
							insertPartitionStatement.setString(2, partition.getLocations().get(i));
							insertPartitionStatement.setString(3, partition.getPartitionColumns().get(j));
							insertPartitionStatement.setInt(4, partition.getpNum());
							insertPartitionStatement.setLong(5, partition.getSize());
							insertPartitionStatement.addBatch();
						}

					}
				}
			}
			insertPartitionStatement.executeBatch();

			for (Index index : table.getIndexes()) {
				for (int i = index.getParitions().nextSetBit(0); i >= 0; i = index.getParitions().nextSetBit(i + 1)) {
					insertIndexStatemenet.setString(1, index.getIndexName());
					insertIndexStatemenet.setString(2, index.getTableName());
					insertIndexStatemenet.setString(3, index.getColumnName());
					insertIndexStatemenet.setInt(4, i);
					insertIndexStatemenet.addBatch();
				}
			}
			insertIndexStatemenet.executeBatch();

			insertSqlStatement.close();
			insertPartitionStatement.close();
			insertIndexStatemenet.close();
		} catch (SQLException ex) {
			log.error(ex.getMessage(), ex);
		}

	}

	public PhysicalTable removePhysicalTable(String name) {
		PhysicalTable returnPhysicalTable = getPhysicalTable(name);
		try (Statement deleteIndexStatement = regConn.createStatement();
				Statement deletePartitionStatement = regConn.createStatement();
				Statement deleteSqlStatement = regConn.createStatement()) {
			deleteIndexStatement.execute("DELETE FROM table_index WHERE table_name = '" + name + "'");

			deletePartitionStatement.execute("DELETE FROM partition WHERE table_name = '" + name + "'");

			deleteSqlStatement.execute("DELETE FROM sql WHERE table_name = '" + name + "'");

		} catch (SQLException ex) {
			log.error(ex.getMessage(), ex);
		}

		return returnPhysicalTable;
	}

	public boolean containsPhysicalTable(String name) {
		boolean ret = false;

		try (Statement statement = regConn.createStatement()) {
			ResultSet rs = statement
					.executeQuery("SELECT table_name " + "FROM sql " + "WHERE table_name = '" + name + "';");

			if (rs.next())
				ret = true;

		} catch (SQLException ex) {
			log.error(ex.getMessage(), ex);
		}

		return ret;
	}

	public void pin(String tableName) {

		String psString = "UPDATE sql SET pin = pin+1, last_access=?, num_of_access=num_of_access+1 " +
				// String psString = "UPDATE sql SET pin = pin+1 " +
				"WHERE table_name=?";
		try (PreparedStatement ps = regConn.prepareStatement(psString)) {
			ps.setString(2, tableName);
			ps.setString(1, madgik.exareme.master.engine.remoteQuery.impl.utility.Date.getCurrentDateTime());
			ps.executeUpdate();
		} catch (SQLException ex) {
			log.error(ex.getMessage(), ex);
		}

	}

	public void updateCacheForTableUse(List<Table> tables) {

		String psString = "UPDATE sql SET last_access=?, num_of_access=num_of_access+1 " + "WHERE table_name=?";
		for (Table table : tables) {

            System.out.println("updatess "+table.getName());
            try (PreparedStatement ps = regConn.prepareStatement(psString)) {
				ps.setString(2, table.getName());
				ps.setString(1, madgik.exareme.master.engine.remoteQuery.impl.utility.Date.getCurrentDateTime());
				ps.executeUpdate();
			} catch (SQLException ex) {
				log.error(ex.getMessage(), ex);
			}
		}
	}

	public Map<String, Long> getWorkersSize() {

		Map<String, Long> sizeMap = new HashMap<>();

		try (Statement statement = regConn.createStatement()) {
			ResultSet rs = statement
					.executeQuery("SELECT `location`, SUM(partition.`size`) AS `size` FROM partition, sql "
							+ "WHERE partition.`table_name`=sql.`table_name` AND sql.`isTemporary`=1 "
							+ " GROUP BY `location` ORDER BY `location`;");

			while (rs.next())
				sizeMap.put(rs.getString(1), rs.getLong(2));

        } catch (SQLException ex) {
			log.error(ex.getMessage(), ex);
		}

		return sizeMap;
	}

    public Map<String, Long> getWorkersSize(List<String> evictedTables) {

        Map<String, Long> sizeMap = new HashMap<>();

        StringBuilder query = new StringBuilder("SELECT `location`, SUM(partition.`size`) " +
                "AS `size` FROM partition, sql " +
                "WHERE partition.`table_name`=sql.`table_name` AND sql.`isTemporary`=1 " +
                " AND sql.`table_name` not in (");
        boolean firstEvictedTAble = true;
        for(String evictedTable : evictedTables){
            if(!firstEvictedTAble){
                query.append(" union select \"" + evictedTable +"\"");
            }else{
                firstEvictedTAble = false;
                query.append(" select \"" + evictedTable +"\"");
            }
        }
        query.append(") GROUP BY `location` ORDER BY `location`;");
        System.out.println("query pou dinw "+query);

        try (Statement statement = regConn.createStatement()) {
            ResultSet rs = statement.executeQuery(query.toString());

            while (rs.next())
                sizeMap.put(rs.getString(1), rs.getLong(2));

        } catch (SQLException ex) {
            log.error(ex.getMessage(), ex);
        }

        return sizeMap;
    }

	public Map<String, Map<String, Long>> getSizePerQuery() {

		Map<String, Map<String, Long>> sizeMap = new HashMap<>();
		Map<String, Long> map;

		try (Statement statement = regConn.createStatement()) {
			ResultSet rs = statement
					.executeQuery("SELECT `table_name`, `location`, SUM(`size`) AS `size` FROM partition "
							+ "GROUP BY `table_name`, `location`;");

			while (rs.next()) {
				if (sizeMap.containsKey(rs.getString(1))) {
					map = sizeMap.get(rs.getString(1));
					map.put(rs.getString(2), rs.getLong(3));
				} else {
					map = new HashMap<>();
					map.put(rs.getString(2), rs.getLong(3));
					sizeMap.put(rs.getString(1), map);
				}
			}

		} catch (SQLException ex) {
			log.error(ex.getMessage(), ex);
		}

		return sizeMap;
	}

	public List<Table> getTemporaryTablesCacheInfo() {

		List<Table> tableInfoList = new LinkedList<>();
		Table table = null;

		try (Statement statement = regConn.createStatement()) {
			ResultSet rs = statement.executeQuery(
					"SELECT `table_name`, `size`, `last_access`, `num_of_access`, `pin` FROM sql WHERE `isTemporary`=1;");

			while (rs.next()) {
				table = new Table(rs.getString(1));
				table.setSize(rs.getInt(2));
				table.setLastAccess(rs.getString(3));
				table.setNumOfAccess(rs.getInt(4));
				table.setPin(rs.getInt(5));
				tableInfoList.add(table);
			}

		} catch (SQLException ex) {
			log.error(ex.getMessage(), ex);
		}

		return tableInfoList;

	}

	public void unpin(String tableName) {

		String psString = "UPDATE sql SET pin = pin-1 WHERE table_name=? AND isTemporary=1";
		try (PreparedStatement ps = regConn.prepareStatement(psString)) {
			ps.setString(1, tableName);
			ps.executeUpdate();
		} catch (SQLException ex) {
			log.error(ex.getMessage(), ex);
		}

	}

	public String containsQuery(String query) {

		String tableName = null;
		try (Statement statement = regConn.createStatement()) {
			ResultSet rs = statement
					.executeQuery("SELECT table_name " + "FROM sql " + "WHERE sqlQuery = '" + query + "';");

			if (rs.next())
				tableName = rs.getString(1);

		} catch (SQLException ex) {
			log.error(ex.getMessage(), ex);
		}

		return tableName;
	}

	public String containsHashID(int hashID) {

		String tableName = null;
		try (Statement statement = regConn.createStatement()) {
			ResultSet rs = statement
					.executeQuery("SELECT table_name " + "FROM sql " + "WHERE hashID = '" + hashID + "';");

			if (rs.next())
				tableName = rs.getString(1);

		} catch (SQLException ex) {
			log.error(ex.getMessage(), ex);
		}

		return tableName;
	}

	public String containsHashID(int hashID, long validDuration) throws ParseException {

		String tableName = null;
		try (Statement statement = regConn.createStatement()) {
			ResultSet rs = statement.executeQuery(
					"SELECT table_name, storage_time " + "FROM sql " + "WHERE hashID = '" + hashID + "';");

			if (rs.next()) {
				tableName = rs.getString(1);
				SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				java.util.Date currentTime = format
						.parse(madgik.exareme.master.engine.remoteQuery.impl.utility.Date.getCurrentDateTime());
				java.util.Date storageTime = format.parse(rs.getString(2));
				long diffInSeconds = (currentTime.getTime() - storageTime.getTime()) / 1000;
				if (diffInSeconds > validDuration) {
					tableName = null;
				}
			}

		} catch (SQLException ex) {
			log.error(ex.getMessage(), ex);
		}

		return tableName;
	}

	public int getNumOfPartitions(String tableName) {

		int numOfPartitions = 0;
		try (Statement statement = regConn.createStatement()) {
			ResultSet rs = statement
					.executeQuery("SELECT COUNT(*) FROM partition WHERE table_name='" + tableName + "';");

			if (rs.next())
				numOfPartitions = rs.getInt(1);

		} catch (SQLException ex) {
			log.error(ex.getMessage(), ex);
		}
		return numOfPartitions;
	}

	public String getPartitionColumn(String tableName) {

		String partitionColumn = null;
		try (Statement statement = regConn.createStatement()) {
			ResultSet rs = statement
					.executeQuery("SELECT partition_column FROM sqlinfo WHERE table_name='" + tableName + "';");

			if (rs.next())
				partitionColumn = rs.getString(1);

		} catch (SQLException ex) {
			log.error(ex.getMessage(), ex);
		}
		return partitionColumn;
	}

	public PhysicalTable getPhysicalTable(String name) {
		PhysicalTable returnPhysicalTable = null;

		try (Statement getSqlStatement = regConn.createStatement();
				Statement getSqlInfoStatement = regConn.createStatement();
				Statement getIndexStatement = regConn.createStatement();
				Statement getPartitionStatement = regConn.createStatement()) {

			ResultSet rs = getSqlStatement.executeQuery("SELECT * FROM sql " + "WHERE table_name = '" + name + "'");

			if (rs.next()) {
				Table table = new Table(rs.getString("table_name"));
				table.setSqlDefinition(rs.getString("sql_definition"));
				table.setHashID(rs.getBytes("hashID"));
				table.setSize(rs.getLong("size"));
				table.setPin(rs.getInt("pin"));
				table.setLastAccess(rs.getString("last_access"));
				table.setStorageTime(rs.getString("storage_time"));
				table.setSqlQuery(rs.getString("sqlQuery"));

				returnPhysicalTable = new PhysicalTable(table);
			}
			rs.close();

			rs = getSqlInfoStatement.executeQuery("SELECT * FROM sqlinfo " + "WHERE table_name = '" + name + "'");

			if (rs.next()) {
				returnPhysicalTable.addPartitionColumn(rs.getString("partition_column"));
			}
			rs.close();

			rs = getPartitionStatement.executeQuery("SELECT partition_number, " + "partition_column, location, size "
					+ "FROM partition " + "WHERE table_name = '" + name + "' "
					+ "GROUP BY partition_number, partition_column, location;");

			int pNum = -1;
			Partition part = null;
			while (rs.next()) {
				int tempPNum = rs.getInt("partition_number");
				if (pNum != tempPNum) {
					pNum = tempPNum;
					if (part != null)
						returnPhysicalTable.addPartition(part);

					part = new Partition(returnPhysicalTable.getTable().getName(), pNum);
				}

				part.addLocation(rs.getString("location"));
				part.setSize(rs.getLong("size"));
				part.addPartitionColumn(rs.getString("partition_column"));
			}
			if (part != null)
				returnPhysicalTable.addPartition(part);
			rs.close();

			rs = getIndexStatement.executeQuery(
					"SELECT DISTINCT index_name " + "FROM table_index " + "WHERE table_name = '" + name + "';");

			while (rs.next()) {
				// TODO: one query
				Index index = getIndex(rs.getString("index_name"));
				returnPhysicalTable.addIndex(index);
			}

		} catch (SQLException ex) {
			log.error(ex.getMessage(), ex);
		}

		return returnPhysicalTable;
	}

	public Collection<PhysicalTable> getPhysicalTables() {
		List<PhysicalTable> list = new ArrayList<PhysicalTable>();

		try (Statement statement = regConn.createStatement()) {
			ResultSet rs = statement.executeQuery("SELECT table_name FROM sql;");

			while (rs.next()) {
				list.add(getPhysicalTable(rs.getString("table_name")));
			}

		} catch (SQLException ex) {
			log.error(ex.getMessage(), ex);
		}

		return list;
	}

	public void addIndex(Index idx) {
		try (PreparedStatement insertIndexStatemenet = regConn.prepareStatement("INSERT INTO table_index(index_name, "
				+ "table_name, " + "column_name, " + "partition_number) " + "VALUES(?, ?, ?, ?)")) {
			for (int i = idx.getParitions().nextSetBit(0); i >= 0; i = idx.getParitions().nextSetBit(i + 1)) {
				insertIndexStatemenet.setString(1, idx.getIndexName());
				insertIndexStatemenet.setString(2, idx.getTableName());
				insertIndexStatemenet.setString(3, idx.getColumnName());
				insertIndexStatemenet.setInt(4, i);
				insertIndexStatemenet.addBatch();
			}

			insertIndexStatemenet.executeBatch();

		} catch (SQLException ex) {
			log.error(ex.getMessage(), ex);
		}
	}

	private boolean containesIndex(String name) {
		boolean ret = false;

		try (Statement statement = regConn.createStatement()) {
			ResultSet rs = statement
					.executeQuery("SELECT table_name " + "FROM table_index " + "WHERE index_name = '" + name + "';");

			if (rs.next())
				ret = true;
		} catch (SQLException ex) {
			log.error(ex.getMessage(), ex);
		}

		return ret;
	}

	private Index getIndex(String name) {
		Index index = null;
		try (Statement statement = regConn.createStatement()) {
			ResultSet rs = statement.executeQuery("SELECT * FROM table_index " + "WHERE index_name = '" + name + "' "
					+ "GROUP BY table_name, " + "column_name, " + "index_name, " + "partition_number;");

			boolean first = true;
			while (rs.next()) {
				if (first) {
					index = new Index(rs.getString("table_name"), rs.getString("column_name"),
							rs.getString("index_name"));
					first = false;
				}

				index.addPartition(rs.getInt("partition_number"));
			}

		} catch (SQLException ex) {
			log.error(ex.getMessage(), ex);
		}

		return index;
	}

	public String getMappings() {
		// throw new UnsupportedOperationException("Not supported yet.");
		return null;
	}

	public void setMappings(String mappings) {
		// throw new UnsupportedOperationException("Not supported yet.");
	}

	public List<Object[]> getMetadata() {
		List<Object[]> metatada = new ArrayList<>();
		try (Statement statement = regConn.createStatement()) {
			ResultSet rs = statement.executeQuery("SELECT table_name, sql_definition FROM sql;");

			while (rs.next()) {
				Object[] tuple = new Object[2];
				tuple[0] = rs.getString("table_name");
				tuple[1] = rs.getString("sql_definition");
				metatada.add(tuple);
			}

			rs.close();
		} catch (SQLException ex) {
			log.error(ex.getMessage(), ex);
		}

		return metatada;
	}

	public static class Schema implements Serializable {
		private static final long serialVersionUID = 1L;

		private String database = null;
		private HashMap<String, PhysicalTable> tables = null;

		// private HashMap<String, Index> indexes = null;
		// private String mappings = null;

		public Schema(String database, Collection<PhysicalTable> physicalTables) {
			this.database = database;
			this.tables = new HashMap<String, PhysicalTable>();

			for (PhysicalTable table : physicalTables) {
				this.tables.put(table.getName(), table);
			}
		}

		public String getDatabase() {
			return database;
		}

		public PhysicalTable getPhysicalTable(String name) {
			return tables.get(name);
		}

		public Collection<PhysicalTable> getPhysicalTables() {
			return tables.values();
		}
	}

	public void setProperties(AdpDBClientProperties properties) {
		this.properties = properties;
	}
	
    public Triple<String, String, Integer> containsHashIDInfo(int hashID){
        try (Statement statement = regConn.createStatement()) {
            ResultSet rs = statement.executeQuery(
                    "SELECT sql.table_name, partition_column, COUNT(*) FROM sql, partition WHERE " +
                            "hashID="+hashID+" AND partition.table_name=sql.table_name " +
                            "GROUP BY sql.table_name, partition_column;");

            if (rs.next())
                return new Triple<String,String, Integer>(rs.getString(1), rs.getString(2), rs.getInt(3));

        } catch (SQLException ex) {
            log.error(ex.getMessage(), ex);
        }
        return null;

    }

    public Triple<String, String, Integer> containsHashIDInfo(int hashID, long validDuration){
        try (Statement statement = regConn.createStatement()) {
            ResultSet rs = statement.executeQuery(
                    "SELECT sql.table_name, partition_column, COUNT(*), storage_time FROM sql, partition WHERE " +
                            "hashID="+hashID+" AND partition.table_name=sql.table_name " +
                            "GROUP BY sql.table_name, partition_column;");

            if (rs.next()) {
                Triple<String, String, Integer> info = new Triple<String, String, Integer>(rs.getString(1), rs.getString(2), rs.getInt(3));

                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                java.util.Date currentTime = format.parse(madgik.exareme.master.engine.remoteQuery.impl.utility.Date.getCurrentDateTime());
                java.util.Date storageTime = format.parse(rs.getString(4));
                long diffInSeconds = (currentTime.getTime() - storageTime.getTime()) / 1000;
                if (diffInSeconds <= validDuration) {
                    return info;
                }
            }

        } catch (SQLException ex) {
            log.error(ex.getMessage(), ex);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;

    }

}

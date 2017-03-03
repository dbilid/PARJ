package madgik.exareme.master.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import java.sql.PreparedStatement;

import madgik.exareme.master.queryProcessor.decomposer.query.SQLQuery;

public class SQLiteLocalExecutor implements Runnable {
	private Connection con;
	private SQLQuery sql;
	private int partition;
	private boolean temp;
	private Set<Integer> finishedQueries;
	private ResultBuffer globalBuffer;
	private static final Logger log = Logger.getLogger(SQLiteLocalExecutor.class);

	public void setGlobalBuffer(ResultBuffer globalBuffer) {
		this.globalBuffer = globalBuffer;
	}

	public SQLiteLocalExecutor(SQLQuery result, Connection c, boolean t, Set<Integer> f, int pt) {
		this.sql = result;
		this.con = c;
		this.temp = t;
		this.finishedQueries = f;
		this.partition = pt;
		// System.out.println(sql);
	}

	@Override
	public void run() {
		execute();

		// if (!temp) {
		synchronized (finishedQueries) {
			finishedQueries.add(partition);
			finishedQueries.notifyAll();
		}
		// }

	}

	private void execute() {
		Statement st;
		try {
			System.out.println("starting thread");
			// st=con.createStatement();
			if (temp) {

				con.setAutoCommit(false);
				st = con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				st.setFetchSize(1000);
				long lll = System.currentTimeMillis();
				String sqlString=sql.getSqlForPartition(partition);
				if(sqlString==null){
					synchronized (globalBuffer) {
						globalBuffer.addFinished();
						globalBuffer.notifyAll();
					}
					return;
				}
				//System.out.println(sqlString);
				ResultSet rs = st.executeQuery(sql.getSqlForPartition(partition));
				int columns = rs.getMetaData().getColumnCount();
				List<List<Object>> localBuffer = new ArrayList<List<Object>>(9000);
				int counter = 0;
				//boolean print=false;
				while (rs.next()) {
					
					// if(counter==1){
					// System.out.println("started");
					// }
					if (counter == 8999) {
						counter = 0;
						synchronized (globalBuffer) {
							while (globalBuffer.size() > 9000) {
								try {
									globalBuffer.wait();
								} catch (InterruptedException e) {
									// System.out.println("local inter");
								}
							}
							// System.out.println("adding batch");
							globalBuffer.addAll(localBuffer);
							globalBuffer.notifyAll();
						}
						localBuffer.clear();
					}
					//if(!print)
					//	continue;
					List<Object> tuple = new ArrayList<Object>(columns);
					for (int i = 1; i < columns + 1; i++) {
						tuple.add(rs.getObject(i));
					}
					localBuffer.add(tuple);
					counter++;
				}
				System.out.println("thread executed in:" + (System.currentTimeMillis() - lll) + " ms");
				synchronized (globalBuffer) {
					while (globalBuffer.size() > 9000) {
						try {
							globalBuffer.wait();
						} catch (InterruptedException e) {
						}
					}
					globalBuffer.addAll(localBuffer);
					globalBuffer.addFinished();
					globalBuffer.notifyAll();
				}
				st.close();
				// con.close();
				localBuffer.clear();

			} else {
				// System.out.println(s.toSQL());
				PreparedStatement ps = con.prepareStatement(sql.getSqlForPartition(partition));
				ps.execute();
			}

			con.close();
			System.out.println("thread finished");
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}

	}
}
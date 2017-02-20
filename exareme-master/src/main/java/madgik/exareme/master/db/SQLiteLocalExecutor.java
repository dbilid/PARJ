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
	private String sql;
	private int partition;
	private boolean temp;
	private Set<Integer> finishedQueries;
	private ResultBuffer globalBuffer;
	private static final Logger log = Logger.getLogger(SQLiteLocalExecutor.class);

	public void setGlobalBuffer(ResultBuffer globalBuffer) {
		this.globalBuffer = globalBuffer;
	}

	public SQLiteLocalExecutor(String sql, Connection c, boolean t, Set<Integer> f, int pt) {
		this.sql = sql;
		this.con = c;
		this.temp = t;
		this.finishedQueries = f;
		this.partition=pt;
	}

	@Override
	public void run() {
			execute();
	
		//if (!temp) {
			synchronized (finishedQueries) {
				finishedQueries.add(partition);
				finishedQueries.notifyAll();
			}
		//}

	}

	private void execute() {
		Statement st;
		try {
			System.out.println("start");
			//st=con.createStatement();
			if (temp) {
			
				con.setAutoCommit(false);
				st = con.createStatement(ResultSet.TYPE_FORWARD_ONLY,
						ResultSet.CONCUR_READ_ONLY);
				st.setFetchSize(10000);
				long lll=System.currentTimeMillis();
				ResultSet rs=st.executeQuery(sql);
				int columns=rs.getMetaData().getColumnCount();
				List<List<Object>> localBuffer=new ArrayList<List<Object>>(1000);
				int counter=0;
				while(rs.next()){
					//if(counter==1){
					//	System.out.println("started");
					//}
					if(counter==999){
						counter=0;
						synchronized(globalBuffer){
							while(globalBuffer.size()>9000){
								try {
									globalBuffer.wait();
								} catch (InterruptedException e) {
									//System.out.println("local inter");
								}
							}
							//System.out.println("adding batch");
							globalBuffer.addAll(localBuffer);
							globalBuffer.notifyAll();
						}
						localBuffer.clear();
					}
					List<Object> tuple=new ArrayList<Object>(columns);
					for(int i=1;i<columns+1;i++){
						tuple.add(rs.getObject(i));
					}
				localBuffer.add(tuple);
					counter++;
				}
				System.out.println("time:"+(lll-System.currentTimeMillis()));
				synchronized(globalBuffer){
					while(globalBuffer.size()>9000){
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
				//con.close();
				localBuffer.clear();
				
			} else {
				// System.out.println(s.toSQL());
				PreparedStatement ps = con.prepareStatement(sql);
				ps.execute();
			}
			
			//connection.close();
			System.out.println("end");
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
		
	}
}
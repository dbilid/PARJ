package madgik.exareme.master.db;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class FinalUnionExecutor implements Runnable {
	private ResultBuffer resultBuffer;
	private PreparedStatement st;
	private int noOfUnions;
	private int results;

	public FinalUnionExecutor(ResultBuffer resultBuffer, PreparedStatement s, int unions) {
		super();
		this.resultBuffer = resultBuffer;
		this.st=s;
		this.noOfUnions=unions;
		this.results=0;
	}

	@Override
	public void run() {
		
			System.out.println("abla");
		synchronized (resultBuffer) {
			while(true){
			System.out.println("yep");
			while (!resultBuffer.isEmpty()) {
				List<Object> tuple = resultBuffer.getNext();
				//System.out.println(tuple);
				results++;
				//for(int i=1;i<tuple.size()+1;i++){
					
				//		st.setObject(i, tuple.get(i-1));
					
				//}
				//st.addBatch();
			}
			System.out.println("executing batch");
			//System.out.println(resultBuffer.size());
			//st.executeBatch();
			resultBuffer.notifyAll();
			if(resultBuffer.getFinished()==noOfUnions){
				System.out.println("results:"+results);
				return;
			}
			try {
				System.out.println("final waiting");
				resultBuffer.wait();
			} catch (InterruptedException e) {
				//System.out.println("inter");
			}
		}
		}
		
	}

}

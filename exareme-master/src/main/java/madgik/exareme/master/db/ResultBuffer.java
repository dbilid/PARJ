package madgik.exareme.master.db;

import java.util.ArrayList;
import java.util.List;

public class ResultBuffer {
	
	private List<List<Object>> buffer;
	private int counter=0;
	public ResultBuffer() {
		super();
		buffer = new ArrayList<List<Object>>(10000);
		counter =0;
	}
	
	public List<Object> getNext(){
		return buffer.remove(0);
	}
	public int getFinished(){
		return counter;
	}
	public void addFinished(){
		counter++;
		System.out.println("finished:"+counter);
	}

	public boolean isEmpty() {
		return buffer.isEmpty();
	}

	public void addAll(List<List<Object>> tuples) {
		this.buffer.addAll(tuples);
		
	}

	public int size() {
		return buffer.size();
	}
	

}

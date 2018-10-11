package madgik.exareme.master.db;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class ResultBuffer {

	private LinkedList<List<Object>> buffer;
	private long counter = 0;

	public ResultBuffer() {
		super();
		buffer = new LinkedList<List<Object>>();
		counter = 0;
	}

	public List<Object> getNext() {
		return buffer.removeFirst();
	}

	public long getFinished() {
		return counter;
	}

	public void addFinished() {
		counter++;
		System.out.println("finished:" + counter);
	}
	
	public void addFinished(long count) {
		counter+=count;
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

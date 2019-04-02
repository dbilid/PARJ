package madgik.exareme.master.queryProcessor.analyzer.stat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JoinCardinalities {
	
	Map<List<Integer>, int[]> cardinalities;

	public JoinCardinalities() {
		super();
		cardinalities=new HashMap<List<Integer>, int[]>();
				
	}
	
	public void add(int relation1, int relation2, int ssSize, int soSize, int osSize, int ooSize){
		//relations must be given ordered by id
		List<Integer> relations=new ArrayList<Integer>(2);
		relations.add(relation1);
		relations.add(relation2);
		int sizes[]=new int[4];
		sizes[0]=ssSize;
		sizes[1]=soSize;
		sizes[2]=osSize;
		sizes[3]=ooSize;
		cardinalities.put(relations, sizes);
	}

	@Override
	public String toString() {
		StringBuffer sb=new StringBuffer();
		for(List<Integer> tables:cardinalities.keySet()){
			int[] info=cardinalities.get(tables);
			sb.append("[");
			sb.append(tables.get(0));
			sb.append(", ");
			sb.append(tables.get(1));
			sb.append(", ");
			sb.append(info[0]);
			sb.append(", ");
			sb.append(info[1]);
			sb.append(", ");
			sb.append(info[2]);
			sb.append(", ");
			sb.append(info[3]);
			sb.append("]\n");
		}
		return sb.toString();
	}

	public int[]  getSizesForTables(List<Integer> tables) {
		return cardinalities.get(tables);
		
	}

	public void addAll(JoinCardinalities temp) {
		cardinalities.putAll(temp.cardinalities);
		
	}
	
	
	
	

}

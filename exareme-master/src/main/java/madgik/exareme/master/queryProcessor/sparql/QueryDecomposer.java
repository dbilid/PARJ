package madgik.exareme.master.queryProcessor.sparql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import madgik.exareme.master.queryProcessor.decomposer.dag.Node;

public class QueryDecomposer {
	
	private String queryString;

	public QueryDecomposer(String queryString) {
		super();
		this.queryString = queryString;
	}
	
	public Node getTopNode(){
		Map<String, String> prefixMap=new HashMap<String, String>();
		String[] prefixBody=queryString.split("SELECT");
		if(prefixBody[0].contains("PREFIX")){
			String[] prefixes=prefixBody[0].split("PREFIX");
			for(int i=0;i<prefixes.length;i++){
				String[] prefix=prefixes[0].split(":");
				prefix[1]=prefix[1].replace("<", "").replace(">", "").replaceAll(" ", "");
				prefixMap.put(prefix[0].replaceAll(" ", ""), prefix[1]);
				
			}
			parseBody(prefixMap, prefixBody[1]);
		}
		else{
			parseBody(prefixMap, prefixBody[0]);
		}
		
		return null;
		
	}

	private void parseBody(Map<String, String> prefixMap, String body) {
			if(body.contains("FROM")){
				String[] varBGP=body.split("FROM");
				String[] vars=varBGP[0].split("?");
				List<String> varList=new ArrayList<String>();
				for(int i=0;i<vars.length;i++){
					varList.add(vars[i].replace("?", "").replaceAll(" ", ""));
				}
				String bgp=varBGP[1].replace("{", "").replace("}", "");
				String[] tps=bgp.split(".");
				
				
			}
		
	}

}

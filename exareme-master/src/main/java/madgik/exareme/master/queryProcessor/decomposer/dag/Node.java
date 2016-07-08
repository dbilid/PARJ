/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package madgik.exareme.master.queryProcessor.decomposer.dag;

import madgik.exareme.master.queryProcessor.decomposer.query.Column;
import madgik.exareme.master.queryProcessor.decomposer.query.NonUnaryWhereCondition;
import madgik.exareme.master.queryProcessor.decomposer.query.Operand;
import madgik.exareme.master.queryProcessor.decomposer.query.Table;
import madgik.exareme.master.queryProcessor.decomposer.util.Pair;
import madgik.exareme.master.queryProcessor.estimator.NodeInfo;

import java.util.*;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 * @author dimitris
 */
public class Node implements Comparator<Node>, Comparable<Node>{

	public static final int OR = 0;
	public static final int AND = 1;
	public static final int JOIN = 0;
	public static final int PROJECT = 1;
	public static final int UNION = 2;
	public static final int GROUPBY = 3;
	public static final int ORDERBY = 4;
	public static final int LIMIT = 5;
	public static final int SELECT = 6;
	public static final int REPARTITION = 7;
	public static final int BROADCAST = 9;
	public static final int BASE = 8;
	public static final int NO_OP = 10;
	public static final int RIGHTBROADCASTJOIN = 11;
	public static final int LEFTBROADCASTJOIN = 12;
	public static final int REPARTITIONJOIN = 13;
	public static final int UNIONALL = 14;
	public static final int NESTED = 15;
	public static final int BASEPROJECT = 16;
	public static final int LEFTJOIN = 17;
	public static final int RIGHTJOIN = 18;
	public static final int JOINKEY = 19;
	public static final int CENTRALIZEDJOIN = 13;
	// private boolean isBaseTable;
	private int type;
	private int prunningCounter;
	// private int parentCounter;
	// private int hashID;
	private List<Node> children;
	private List<Node> parents;
	private Set<String> descendantBaseTables;
	private int opCode;
	private Object o;
	private boolean expanded;
	private boolean shareableComputed;
	private boolean hashNeedsRecomputing;
	private Set<PartitionCols> partitionedColumns;
	private Set<PartitionCols> isBottomNodeForPruningColumns;
	private PartitionRecord partitionRecord;
	// private PartitionCols lastPartition;
	// private boolean prune;
	private Set<PruningInfo> pi;
	private boolean isMaterialised;
	private HashCode hash;
	private Set<Integer> unions;

	// private Set<Column> redundantRepartitions;

	public Node(int type) {
		this.hashNeedsRecomputing = true;
		shareableComputed=false;
		this.type = type;
		this.opCode = -1;
		prunningCounter = 0;
		// parentCounter=0;
		children = new ArrayList<Node>();
		parents = new ArrayList<Node>();
		// isBaseTable=false;
		this.o = new Object();
		this.expanded = false;
		this.partitionedColumns = new HashSet<PartitionCols>();
		this.partitionRecord = new PartitionRecord();
		this.isBottomNodeForPruningColumns = new HashSet<PartitionCols>();
		// prune = false;
		// lastPartition = new PartitionCols();
		pi = new HashSet<PruningInfo>();
		isMaterialised = false;
		descendantBaseTables = new HashSet<String>();
		unions = new HashSet<Integer>();
		// redundantRepartitions=new HashSet<Column>();
	}

	public Node(int type, int opCode) {
		hashNeedsRecomputing = true;
		prunningCounter = 0;
		// parentCounter=0;
		this.opCode = opCode;
		this.type = type;
		this.o = new Object();
		children = new ArrayList<Node>();
		parents = new ArrayList<Node>();
		this.expanded = false;
		this.partitionedColumns = new HashSet<PartitionCols>();
		this.partitionRecord = new PartitionRecord();
		this.isBottomNodeForPruningColumns = new HashSet<PartitionCols>();
		// prune = false;
		// lastPartition = new PartitionCols();
		pi = new HashSet<PruningInfo>();
		isMaterialised = false;
		descendantBaseTables = new HashSet<String>();
		unions = new HashSet<Integer>();
		// redundantRepartitions=new HashSet<Column>();
	}

	/**
	 * @return the type
	 */
	public int getType() {
		return type;
	}

	/**
	 * @param type
	 *            the type to set
	 */
	public void setType(int type) {
		this.type = type;
		this.setHashNeedsRecomputing();

	}

	private NodeInfo nodeInfo;

	/**
	 * @return the hashID
	 */
	public HashCode computeHashID() {

		List<HashCode> codes = new ArrayList<HashCode>();
		codes.add(Hashing.sha1().hashInt(opCode));
		for (Node c : this.children) {
			codes.add(c.getHashId());
		}
		if (o instanceof Table) {
			Table t = (Table) o;
			if (!t.getName().startsWith("table")) {
				// include in hash code only if not temporary tablename
				codes.add(t.getHashID());
			}
			this.hash = Hashing.combineUnordered(codes);
		} else if (o instanceof Operand) {

			Operand op = (Operand) o;
			codes.add(op.getHashID());
			this.hash = Hashing.combineOrdered(codes);
		} else if (o instanceof String) {
			codes.add(Hashing.sha1().hashBytes(((String) o).getBytes()));
			this.hash = Hashing.combineOrdered(codes);
		} else {
			this.hash = Hashing.combineOrdered(codes);
		}

		this.hashNeedsRecomputing = false;
		return hash;
		// }

	}

	public HashCode getHashId() {
		if (hashNeedsRecomputing) {
			this.computeHashID();
		}
		return this.hash;
	}

	/**
	 * @param hashID
	 *            the hashID to set
	 */
	// public void setHashID(int hashID) {
	// this.hashID = hashID;
	// }
	public void addChild(Node v) {
		if (this.children.contains(v)) {
			// System.out.println("replicate child??");
			return;
		}
		this.children.add(v);
		v.parents.add(this);

		setHashNeedsRecomputing();

		// }
	}

	public void setHashNeedsRecomputing() {

		if (this.hashNeedsRecomputing) {
			return;
		}
		hashNeedsRecomputing = true;
		// for(Node c:this.parents){
		// c.setHashNeedsRecomputing();
		// }

	}

	public void addChildAt(Node v, int i) {
		if (this.children.contains(v)) {
			// System.out.println("replicate child??");
			return;
		}
		this.children.add(i, v);
		v.parents.add(this);
		// if (!(this.getObject() instanceof Table && i > 0)) {
		this.setHashNeedsRecomputing();
		// }
	}

	public Node getChildAt(int i) {
		return this.children.get(i);
	}

	public List<Node> getChildren() {
		return Collections.unmodifiableList(this.children);
	}

	public Object getObject() {
		return this.o;
	}

	// public void setIsBaseTable(boolean b){
	// this.isBaseTable=b;
	// }
	public void setObject(Object obj) {
		this.o = obj;
		this.setHashNeedsRecomputing();
	}

	public void setOperator(int i) {
		this.opCode = i;
		this.setHashNeedsRecomputing();
	}

	public StringBuilder dotPrint(Set<Node> visited) {
		StringBuilder result = new StringBuilder();
		HashSet<String> shapes = new HashSet<String>();
		LinkedList<Node> queue = new LinkedList<Node>();
		
		queue.offer(this); // Place start node in queue
		shapes.add(this.dotShape());
		while (!queue.isEmpty()) {
			Node v = queue.getFirst();
			// Update neighbors
			if (!v.getChildren().isEmpty()) {
				for (int i = 0; i < v.getChildren().size(); i++) {
					Node c = v.getChildren().get(i);
					if(visited.contains(c)){
						continue;
					}
					//visited.add(c);
					queue.add(c);
					shapes.add(c.dotShape());
					result.append(v.dotString());
					result.append(" -> ");
					result.append(c.dotString());
					// result.append("[label=\"").append(i).append("\"]");
					result.append(";\n");
				}

			}
			// else {
			// result.append("type:"+v.type+" opCode:"+v.opCode+"
			// object:"+v.o.toString());
			// }

			queue.removeFirst();
		}
		result.append(" }");

		result.insert(0, " } ");
		Iterator<String> it = shapes.iterator();
		while (it.hasNext()) {
			result.insert(0, it.next());
		}
		result.insert(0, "strict digraph G{ {");
		return result;
	}

	private String dotString() {
		/*
		 * if(this.o instanceof Selection){ return "SELECTION"; } else if(this.o
		 * instanceof Projection){ return "PROJECTION"; } else{
		 */
		return String.valueOf(this.hashCode());
		// }
	}

	private String dotShape() {
		String shape = "oval";
		if (this.type == 0) {
			shape = "box";
		}

		// if(prunningCounter>0){
		// return String.valueOf(this.hashCode()) + "[label= \"" + o.toString()
		// + " d:" + prunningCounter + " p:" + parents.size() + "\"]" +
		// " [shape=" + shape + "]";
		// return String.valueOf(this.hashCode()) + "[label= \"" + o.toString()
		// + " pr:" + this.prune + "\"]" + " [shape=" + shape + "]";
		String color = "black";
		if (!this.isBottomNodeForPruningColumns.isEmpty()) {
			color = "red";
		}
		if (!this.pi.isEmpty()) {
			color = "yellow";
		}
		String materialized = "";
		if (this.isMaterialised) {
			materialized = " [xlabel=\"M\"] ";
		}
		// return String.valueOf(this.hashCode()) + "[label= \"" + o.toString()
		// +" pr:" + this.pi + " cols:" + this.partitionRecord + "\"]"
		// +" [color="+color+ " [shape=" + shape + "]";
		// return String.valueOf(this.hashCode()) + "[label= \"" + o.toString()
		// + "\"]"+materialized +" [color="+color+ "] [shape=" + shape + "]";

		// }

		String object = o.toString();
		String fillcolor = "";
		if (o instanceof Table) {

			Table t = (Table) o;
			if (t.getAlias() == null) {
				// object = "Intermediate Result";
				object = t.getName();
				if (this.parents.isEmpty()) {
					object = "Result";
				}
			} else {
				if (t.getName().startsWith("RECALL_")) {
					fillcolor = " fillcolor=\"yellow\" style=\"filled\"";
				} else if (t.getName().startsWith("SLEGGE_")) {
					fillcolor = " fillcolor=\"red\" style=\"filled\"";
				} else if (t.getName().startsWith("SLEGGE1_")) {
					fillcolor = " fillcolor=\"green\" style=\"filled\"";
				} else if (t.getName().startsWith("OPENWORKSBRAGE_")) {
					fillcolor = " fillcolor=\"green\" style=\"filled\"";
				}
			}
			if (this.nodeInfo != null)
				object += "card:" + this.getNodeInfo().getNumberOfTuples();
		}
		if (this.opCode == LEFTBROADCASTJOIN) {
			object += "L:";
		}
		if (this.opCode == RIGHTBROADCASTJOIN) {
			object += "R:";
		}
		if (this.opCode == PROJECT) {
			object = "Project";
		}
		object += " " + unions.toString();
		return String.valueOf(this.hashCode()) + "[label= \"" + object + "\"" + fillcolor + "]" + " [shape=" + shape
				+ "]";
		// return String.valueOf(this.hashCode()) + "[label= \"" + object + "::"
		// + this.descendantBaseTables.toString() + " id:"
		// + this.getHashId() + "\"]" + " [shape=" + shape + "]";
	}

	public boolean isExpanded() {
		return expanded;
	}

	public void setExpanded(boolean expanded) {
		this.expanded = expanded;
	}



	public int getOpCode() {
		return this.opCode;
	}

	public int removeChild(Node lt) {
		int res = this.children.indexOf(lt);
		boolean r = this.children.remove(lt);
		lt.parents.remove(this);
		if (r) {
			// if (!(this.getObject() instanceof Table && res > 0)) {
			this.setHashNeedsRecomputing();
			// }
		} else {
			System.out.println("child not exists!!");
		}
		return res;
	}

	public void removeChildAt(int i) {
		Node toRemove = this.getChildAt(i);
		toRemove.parents.remove(this);
		this.removeChild(toRemove);
		this.setHashNeedsRecomputing();
	}

	public Set<PartitionCols> isPartitionedOn() {
		return this.partitionedColumns;
	}

	public PartitionCols getFirstPartitionedSet() {
		if (this.partitionedColumns.isEmpty()) {
			return new PartitionCols();
		} else {
			return this.partitionedColumns.iterator().next();
		}
	}

	public void setPartitionedOn(Set<PartitionCols> s) {
		for (PartitionCols pc : s) {
			this.partitionedColumns.add(pc);
		}
	}

	public void setPartitionedOn(PartitionCols pc) {
		this.partitionedColumns.add(pc);

	}

	public void addToPartitionRecord(PartitionCols s) {
		this.partitionRecord.add(s);
	}

	public void addToPartitionRecord(PartitionRecord pr) {
		for (PartitionCols pc : pr.getPartitionCols()) {
			this.partitionRecord.add(pc);
		}

	}

	public void addToPartitionRecord(List<Column> l) {
		PartitionCols pc = new PartitionCols();
		pc.addColumns(l);
		this.partitionRecord.add(pc);
	}

	public void removeAllChildren() {
		for (Node c : this.children) {
			c.parents.remove(this);
			// if(c.parents.isEmpty()){
			// System.out.println("remove?");
			// }
		}
		this.children = new ArrayList<Node>();
		this.setHashNeedsRecomputing();
	}

	public PartitionRecord getPartitionRecord() {
		return this.partitionRecord;
	}

	public void increasePruningCounter() {
		prunningCounter++;
	}

	public boolean prune() {
		return !this.pi.isEmpty();
	}

	int getNoOfParents() {
		return this.parents.size();
	}

	public boolean partitionRecordContains(PartitionCols ptned) {
		for (PartitionCols pc : this.partitionRecord.getPartitionCols()) {
			if (!Collections.disjoint(ptned.getColumns(), pc.getColumns())) {
				return true;
			}
		}
		return false;
	}

	public Node getFirstParent() {
		return this.parents.get(0);
	}

	public List<Node> getParents() {
		return this.parents;
	}

	public int getFirstIndexOfChild(Node c) {
		for (int i = 0; i < children.size(); i++) {
			if (children.get(i).equals(c)) {
				return i;
			}
		}
		return -1;
	}

	public void setPartitionRecord(PartitionRecord partitionCols) {
		this.partitionRecord = new PartitionRecord();
		for (PartitionCols pc : partitionCols.getPartitionCols()) {
			PartitionCols newCols = new PartitionCols();
			newCols.addColumns(pc.getColumns());
			this.partitionRecord.add(newCols);
		}
	}

	/*
	 * public boolean addToPartitionRecordColumns(List<Column> cols) { for
	 * (Column c : cols) { for (PartitionCols pc : this.getPartitionRecord()) {
	 * if (pc != this.lastPartition && pc.contains(c)) { return false; } } }
	 * PartitionCols pr = new PartitionCols(cols); this.partitionRecord.add(pr);
	 * this.lastPartition = pr; return true; }
	 */
	public Set<Pair<PartitionCols, Node>> existsInPreviousPartitionsAndCheckForPruning(Node parent, Column c) {
		Set<Pair<PartitionCols, Node>> result = new HashSet<Pair<PartitionCols, Node>>();
		PartitionCols existing = null;
		// for (Column c : cols) {
		for (PartitionCols pc : this.getPartitionRecord().getPartitionCols()) {
			if (pc.contains(c)) {
				existing = pc;
				break;
			}
		}
		// }
		if (existing != null) {
			Set<Pair<Column, Node>> lastJoiningColumns = this.getChildreJoiningColumns();
			// indicates which PartitionCols are the last one and for which node
			// Set<Pair<PartitionCols, Node>> lastJoiningPartitionSets=new
			// HashSet<Pair<PartitionCols, Node>>();
			for (Pair<Column, Node> p : lastJoiningColumns) {
				PartitionCols pc = partitionRecord.getPartitionColsFor(p.getVar1());
				if (existing != pc) {
					// result.add(new Pair(pc, p.getVar2()));
					PruningInfo prun = new PruningInfo(parent, p.getVar2(), existing);
					this.pi.add(prun);
					p.getVar2().searchForBottomPruningNodes(existing);
				}
				// lastJoiningPartitionSets.add(new
				// Pair(partitionRecord.getPartitionColsFor(p.getVar1()),
				// p.getVar2()));
			}
		}

		return result;
	}

	public void mergePartitionRecords(PartitionRecord otherPartitionRecord, List<Column> joiningCols) {
		// PartitionCols joining = new PartitionCols();
		// joining.addColumns(joiningCols);

		Set<PartitionCols> toRemove = new HashSet<PartitionCols>();
		boolean joiningColsExist = false;
		for (PartitionCols these : this.partitionRecord.getPartitionCols()) {
			if (!Collections.disjoint(these.getColumns(), joiningCols)) {
				these.addColumns(joiningCols);
				// this.setLastPartition(these);
				joiningColsExist = true;
				break;
			}
		}
		if (!joiningColsExist) {
			PartitionCols join = new PartitionCols();
			join.addColumns(joiningCols);
			this.partitionRecord.add(join);
		}
		// toRemove.clear();
		// this.partitionRecord.removeAll(toRemove);
		// this.partitionRecord.add(joining);
		for (PartitionCols these : this.partitionRecord.getPartitionCols()) {
			for (PartitionCols others : otherPartitionRecord.getPartitionCols()) {
				if (!Collections.disjoint(these.getColumns(), others.getColumns())) {
					these.addColumns(others.getColumns());
					toRemove.add(others);
				}
			}
		}
		for (PartitionCols others : otherPartitionRecord.getPartitionCols()) {
			if (!toRemove.contains(others)) {
				this.partitionRecord.add(others);
			}
		}
	}

	public void checkForPruning(Node parent, PartitionCols c) {

		for (Node child : this.children) {
			if (child.getObject() instanceof NonUnaryWhereCondition) {
				NonUnaryWhereCondition nuwc = (NonUnaryWhereCondition) child.getObject();
				boolean addPrunning = true;

				Column j = nuwc.getLeftOp().getAllColumnRefs().get(0);
				for (Column cc : c.getColumns()) {
					if (j.equals(cc)) {
						addPrunning = false;
						break;
					}

				}
				if (addPrunning) {
					PruningInfo prun = new PruningInfo(parent, child, c);
					this.pi.add(prun);
				}
			}
		}

	}

	/*
	 * public void setFlagForRedPartition(Column c) { boolean isTopPruningNode =
	 * true; for (Node p : this.parents) { for (PartitionCols pc :
	 * p.partitionRecord.getPartitionCols()) { if (pc.contains(c)) {
	 * p.setFlagForRedPartition(c); isTopPruningNode = false; } } } if
	 * (isTopPruningNode) { this.prune = true; } }
	 */
	/*
	 * public PartitionCols getLastPartition() { return lastPartition; }
	 * 
	 * public void setLastPartition(PartitionCols lastPartition) {
	 * this.lastPartition = lastPartition; }
	 */

	public boolean isMaterialised() {
		return isMaterialised;
	}

	public void setMaterialised(boolean isMaterialised) {
		this.isMaterialised = isMaterialised;
	}

	public void setPlanMaterialized(Iterator<Integer> planIterator) {
		this.setMaterialised(true);
		// if(planIterator.hasNext()){
		if (!this.children.isEmpty()) {
			Node op = children.get(planIterator.next());
			for (Node eq2 : op.getChildren()) {
				eq2.setPlanMaterialized(planIterator);
			}
		}
	}

	private Set<Pair<Column, Node>> getLastJoiningColumns() {
		// check in all descedants
		Set<Pair<Column, Node>> result = new HashSet<Pair<Column, Node>>();
		if (o instanceof NonUnaryWhereCondition) {
			NonUnaryWhereCondition nuwc = (NonUnaryWhereCondition) o;
			result.add(new Pair(nuwc.getAllColumnRefs().get(0), this));

		} else {
			for (Node c : this.children) {
				Set<Pair<Column, Node>> cm = c.getLastJoiningColumns();
				for (Pair cc : cm) {
					result.add(cc);
				}
			}

		}
		return result;
	}

	private Set<Pair<Column, Node>> getChildreJoiningColumns() {
		// check only in children
		Set<Pair<Column, Node>> result = new HashSet<Pair<Column, Node>>();

		for (Node c : this.children) {
			if (c.o instanceof NonUnaryWhereCondition) {
				NonUnaryWhereCondition nuwc = (NonUnaryWhereCondition) c.o;
				result.add(new Pair(nuwc.getAllColumnRefs().get(0), c));
			}

		}
		return result;
	}

	private boolean searchForBottomPruningNodes(PartitionCols pc) {
		if (!this.partitionRecord.isDisjointWith(pc)) {

			boolean existsInChildren = false;
			for (Node e : this.children) {
				if (e.searchForBottomPruningNodes(pc)) {
					existsInChildren = true;
				}
			}
			if (!existsInChildren) {

				this.isBottomNodeForPruningColumns.add(pc);

			}
			return true;
		} else {
			return false;
		}
	}

	public boolean prune(Node o) {
		for (PruningInfo pruning : pi) {
			if (pruning.child.equals(o)) {
				return true;
			}
		}
		return false;
	}

	public int[] getAlgorithmicImplementations() {
		if (this.o instanceof NonUnaryWhereCondition) {
			int[] result = new int[1];
			// result[0]=RIGHTBROADCASTJOIN;
			// result[1]=LEFTBROADCASTJOIN;
			boolean centralised=false;
			/*if(this.children.size()==2){
				for(Node c:children){
				if(c.nodeInfo!=null && c.nodeInfo.getNumberOfTuples()*c.getNodeInfo().getTupleLength()>300000){
					centralised=false;
					break;
				}
			}
			}*/
			
			if(!centralised){
				result[0] = REPARTITIONJOIN;
			}else{
				result[0] = CENTRALIZEDJOIN;
			}
			return result;
		} else {
			int[] result = new int[1];
			result[0] = this.opCode;
			return result;
		}
	}

	public NodeInfo getNodeInfo() {
		return nodeInfo;
		
	}

	public void setNodeInfo(NodeInfo nodeInfo) {
		this.nodeInfo = nodeInfo;
	}

	public boolean isDescendantOfBaseTable(String alias) {
		return this.descendantBaseTables.contains(alias);
	}

	public void addDescendantBaseTable(String alias) {
		this.descendantBaseTables.add(alias);
	}

	public Set<String> getDescendantBaseTables() {
		return this.descendantBaseTables;
	}

	public void addAllDescendantBaseTables(Set<String> aliases) {
		this.descendantBaseTables.addAll(aliases);
	}

	public int count(int i) {
		if (this.isMaterialised) {
			return 0;
		} else {
			if (this.type == Node.OR) {
				i++;
			}
			this.isMaterialised = true;
			for (Node c : this.children) {
				i += c.count(0);
			}
		}
		// TODO Auto-generated method stub
		return i;
	}

	public Set<Integer> getUnions() {
		return unions;
	}

	public void setUnions(Set<Integer> unions) {
		this.unions = unions;
	}

	public void addUnionToDesc(int counter) {
		if (unions.add(counter)) {
			for (Node c : this.children) {
				c.addUnionToDesc(counter);
			}
		}
	}

	public void addShareable(List<Node> shareable) {
		if(shareableComputed){
			return;
		}
		if (this.unions.size() > 1 && this.type == Node.OR && !shareable.contains(this) && this.getDescendantBaseTables().size()>1) {
			boolean hasFilterJoinParent=false;
			for(Node p:this.parents){
				if(p.getChildren().size()==1&&p.getOpCode()==JOIN){
					hasFilterJoinParent=true;
					break;
				}
			}
			if (!hasFilterJoinParent&&shareable.add(this)) {
				return;
			}
		}
		for (Node c : this.children) {
			c.addShareable(shareable);
		}
		shareableComputed=true;

	}

	public int compareTo(Node other) {
		int thisScore = (getUnions().size() * 10) + getDescendantBaseTables().size();
		int otherScore = (other.getUnions().size() * 10) + other.getDescendantBaseTables().size();
		if (thisScore > otherScore) {
			return 1;
		} else if (otherScore > thisScore) {
			return -1;
		} else {
			return 0;
		}
	}

	public int compare(Node d, Node d1) {
		int thisScore = (d.getUnions().size() * 10) + d.getDescendantBaseTables().size();
		int otherScore = (d1.getUnions().size() * 10) + d1.getDescendantBaseTables().size();
		return thisScore - otherScore;
	}

	@Override
	public String toString() {
		return "Node [opCode=" + opCode + ", o=" + o + "]";
	}


	
	

}

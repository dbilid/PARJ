/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package madgik.exareme.master.queryProcessor.estimator.db;

import madgik.exareme.master.queryProcessor.decomposer.dag.NodeHashValues;
import madgik.exareme.master.queryProcessor.decomposer.query.Column;
import madgik.exareme.master.queryProcessor.estimator.histogram.Bucket;
import madgik.exareme.master.queryProcessor.estimator.histogram.Histogram;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 * @author jim
 */
public class RelInfo {
	private int relName;
	private Map<Column, AttrInfo> attrIndex;
	private double numberOfTuples;
	private int tupleLength;
	private static final org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(RelInfo.class);

	/* constructor */
	public RelInfo(int relName, Map<Column, AttrInfo> attrIndex, double numberOfTuples, int tupleLength) {

		this.relName = relName;
		this.attrIndex = attrIndex;
		this.numberOfTuples = numberOfTuples;
		this.tupleLength = tupleLength;
	}

	/* copy constructor */
	public RelInfo(RelInfo rel) {
		this.relName = rel.getRelName();
		this.numberOfTuples = rel.getNumberOfTuples();
		this.tupleLength = rel.getTupleLength();
		this.attrIndex = new HashMap<Column, AttrInfo>();

		for (Map.Entry<Column, AttrInfo> e : rel.getAttrIndex().entrySet()) {
			if (e.getValue() == null) {
				continue;
			}
			this.attrIndex.put(e.getKey(), new AttrInfo(e.getValue()));
		}
	}

	public RelInfo(RelInfo rel, int tableAlias, boolean isNested) {
		this.relName = tableAlias;
		this.numberOfTuples = rel.getNumberOfTuples();
		this.tupleLength = rel.getTupleLength();
		this.attrIndex = new HashMap<Column, AttrInfo>();
			for (Map.Entry<Column, AttrInfo> e : rel.getAttrIndex().entrySet()) {
				this.attrIndex.put(new Column(tableAlias, e.getKey().getName()), e.getValue());
			}
			
		
	}

	/* getters and seters */
	public int getRelName() {
		return relName;
	}

	public void setRelName(int relName) {
		this.relName = relName;
	}

	public Map<Column, AttrInfo> getAttrIndex() {
		return attrIndex;
	}

	public void setAttrIndex(Map<Column, AttrInfo> attrIndex) {
		this.attrIndex = attrIndex;
	}

	public double getNumberOfTuples() {
		return numberOfTuples;
	}

	public void setNumberOfTuples(double numberOfTuples) {
		this.numberOfTuples = numberOfTuples;
	}

	public int getTupleLength() {
		return tupleLength;
	}

	public void setTupleLength(int tupleLength) {
		this.tupleLength = tupleLength;
	}

	

	public double totalRelSize() {
		return this.numberOfTuples * this.tupleLength;
	}

	/* standard methods */
	@Override
	public String toString() {
		return "Relation{" + "relName=" + relName + ", attrIndex=" + attrIndex + ", numberOfTuples=" + numberOfTuples
				+ ", tupleLength=" + tupleLength + ", hashAttr="
			 + '}';
	}

	/* interface methods */
	// it keeps only essential projected attributes an eliminates the others. It
	// also dicreases the tupleLength.
	public void eliminteRedundantAttributes(Set<Column> projections) {
		Set<Column> remove = new HashSet<Column>();

		for (Column k : this.attrIndex.keySet()) {
			if (!projections.contains(k))
				remove.add(k);
		}

		for (Column k : remove)
			this.attrIndex.remove(k);

		// eliminate redudancy from hashing attributes set
		// for(String k : remove){
		// if(this.hashAttr.contains(k))
		// this.hashAttr.remove(k);
		// }

		refreshTupleLength();
	}

	public void adjustRelation(Column attrName, Histogram h) {
		// difference of the number of records between two tables
		double recTableDiff;
		// percentage of general increasing/decreasing of the rest histograms
		double percentage;
		double numOfTuples = h.numberOfTuples();

		for (AttrInfo attr : this.attrIndex.values()) {
			if (!attr.getAttrName().equals(attrName)) {
				recTableDiff = numOfTuples - attr.getHistogram().numberOfTuples();
				// if(recTableDiff<0){
				// recTableDiff=0;
				// }
				percentage = recTableDiff / attr.getHistogram().numberOfTuples();

				if (Double.isInfinite(percentage)) {
					percentage = Double.MAX_VALUE;
				}
				if (Double.isNaN(percentage)) {
					percentage = 0.0;
				}
				if (h.getBucketIndex().isEmpty()) {
					for (AttrInfo attr2 : this.attrIndex.values()) {
						attr2.getHistogram().getBucketIndex().clear();// ////////////////change
																		// to
																		// default
																		// histogram
					}
					break;
				} else {
					for (Map.Entry<Double, Bucket> entry : attr.getHistogram().getBucketIndex().entrySet()) {
						if (!entry.getValue().equals(Bucket.FINAL_HISTOGRAM_BUCKET)) {
							double frequency = entry.getValue().getFrequency()
									+ percentage * entry.getValue().getFrequency();
							// if(frequency<1.0){
							// frequency=1.0;
							// }
							entry.getValue().setFrequency(frequency);
						}
					}
				}
			}
		}

		// fix new numberOfTUples variable when the histograms changes
		this.numberOfTuples = numOfTuples;
	}

	/* private-helper methods */
	private void refreshTupleLength() {
		int tl = 0;

		for (AttrInfo a : this.attrIndex.values()) {
			if (a == null) {
				log.warn("null RelInfo");
				return;
			}
			tl += a.getAttrLength();
		}

		this.tupleLength = tl;
	}

	public void renameColumn(Column oldName, Column newName) {
		attrIndex.put(newName, attrIndex.get(oldName));
		attrIndex.remove(oldName);

	}

}

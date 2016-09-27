/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package madgik.exareme.master.queryProcessor.decomposer.federation;

/**
 * @author dimitris
 */
public interface MemoValue {


    public SinglePlan getPlan();

    public void setMaterialized(boolean b);

    public boolean isMaterialised();

	public void addUsed(int b);

	public int getUsed();
	
	public boolean isFederated();
	
	public boolean isMultiUsed();

}


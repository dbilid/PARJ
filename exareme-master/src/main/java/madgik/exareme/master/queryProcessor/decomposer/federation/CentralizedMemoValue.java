package madgik.exareme.master.queryProcessor.decomposer.federation;


public class CentralizedMemoValue implements MemoValue {
    private int used;
    private SinglePlan p;
    private boolean materialized;
    private boolean federated;
    private int matUnion;
    private boolean multiUsed;

    public CentralizedMemoValue(SinglePlan p) {
        this.p = p;
        materialized = false;
        used = 0;
        federated = false;
        matUnion=-1;
    }

    public SinglePlan getPlan() {
        return p;
    }

    public void setPlan(SinglePlan p) {
		this.p = p;
	}

	public void setMaterialized(boolean b) {
        this.materialized = b;
    }

    public boolean isMaterialised() {
        return this.materialized;
    }

    public void addUsed(int b) {
        used+=b;
        if(used>1){
        	this.multiUsed=true;
        }
        else{
        	this.multiUsed=false;
        }
    }

    public int getUsed() {
        return used;
    }

    public void setFederated(boolean f) {
        this.federated = f;
    }

    public boolean isFederated() {
        return this.federated;
    }

	public int getMatUnion() {
		return matUnion;
	}

	public void setMatUnion(int matUnion) {
		this.matUnion = matUnion;
	}
    
	public boolean isMultiUsed() {
		return multiUsed;
	}

}

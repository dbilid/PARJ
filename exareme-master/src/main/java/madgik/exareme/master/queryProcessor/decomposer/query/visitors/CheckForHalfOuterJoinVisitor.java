package madgik.exareme.master.queryProcessor.decomposer.query.visitors;

import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.parser.HalfOuterJoinNode;
import com.foundationdb.sql.parser.Visitable;

import madgik.exareme.master.queryProcessor.decomposer.query.SQLQuery;

public class CheckForHalfOuterJoinVisitor extends AbstractVisitor {

	private boolean containsHalfOuterJoin;
	public CheckForHalfOuterJoinVisitor(SQLQuery query) {
		super(query);
		containsHalfOuterJoin=false;
	}

	@Override
	public Visitable visit(Visitable node) throws StandardException {
		if (node instanceof HalfOuterJoinNode) {
			containsHalfOuterJoin=true;
		}
		return node;
	}
	
	@Override public boolean stopTraversal() {
        return containsHalfOuterJoin;
    }

}

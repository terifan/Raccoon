package org.terifan.raccoon.btree;

import org.terifan.raccoon.blockdevice.BlockPointer;


public abstract class BTreeNode
{
	protected BTree mTree;
	protected BTreeInteriorNode mParent;
	protected BlockPointer mBlockPointer;
	protected int mLevel;

	protected static int COUNTER;
	protected final int UNIQUE=++COUNTER;

//	protected boolean mModified;
//	protected boolean mHighlight;
//	protected NodeState mChange;


	protected BTreeNode(BTree aTree, BTreeInteriorNode aParent, int aLevel)
	{
		mTree = aTree;
		mParent = aParent;
		mLevel = aLevel;
	}


	abstract OpResult get(ArrayMapKey aKey);


	abstract OpResult put(ArrayMapKey aKey, ArrayMapEntry aEntry);


	abstract OpResult remove(ArrayMapKey aKey);


	abstract void visit(BTreeVisitor aVisitor, ArrayMapKey aLowestKey, ArrayMapKey aHighestKey);


	abstract void commit();


	protected abstract void postCommit();


	protected abstract String integrityCheck();


	protected abstract int size();


	enum VisitorState
	{
		CONTINUE,
		ABORT,
		SKIP
	}
}

package org.terifan.raccoon;

import org.terifan.raccoon.ArrayMap.PutResult;
import org.terifan.raccoon.blockdevice.BlockPointer;
import org.terifan.raccoon.util.Result;


public abstract class BTreeNode
{
	protected BlockPointer mBlockPointer;
	protected boolean mModified;
	protected int mLevel;
	protected boolean mHighlight;


	static class SplitResult
	{
		private final BTreeNode mLeftNode;
		private final BTreeNode mRightNode;
		private final ArrayMapKey mLeftKey;
		private final ArrayMapKey mRightKey;


		SplitResult(BTreeNode aLeftNode, BTreeNode aRightNode, ArrayMapKey aLeftKey, ArrayMapKey aRightKey)
		{
			mLeftNode = aLeftNode;
			mRightNode = aRightNode;
			mLeftKey = aLeftKey;
			mRightKey = aRightKey;
		}


		public BTreeNode getLeftNode()
		{
			return mLeftNode;
		}


		public ArrayMapKey getLeftKey()
		{
			return mLeftKey;
		}


		public BTreeNode getRightNode()
		{
			return mRightNode;
		}


		public ArrayMapKey getRightKey()
		{
			return mRightKey;
		}
	}


	protected BTreeNode(int aLevel)
	{
		mLevel = aLevel;
	}


	abstract boolean get(BTree aImplementation, ArrayMapKey aKey, ArrayMapEntry oEntry);


	abstract PutResult put(BTree aImplementation, ArrayMapKey aKey, ArrayMapEntry aEntry, Result<ArrayMapEntry> oOldEntry);


	abstract RemoveResult remove(BTree aImplementation, ArrayMapKey aKey, Result<ArrayMapEntry> oOldEntry);


	abstract void visit(BTree aImplementation, BTreeVisitor aVisitor, ArrayMapKey aLowestKey, ArrayMapKey aHighestKey);


	abstract SplitResult split(BTree aImplementation);


	abstract boolean commit(BTree aImplementation);


	protected abstract void postCommit();


	protected abstract String integrityCheck();


	protected abstract int childCount();


	enum RemoveResult
	{
		REMOVED,
		NO_MATCH
	}


	enum VisitorState
	{
		CONTINUE,
		ABORT,
		SKIP
	}
}

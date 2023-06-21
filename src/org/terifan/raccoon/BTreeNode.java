package org.terifan.raccoon;

import org.terifan.raccoon.ArrayMap.PutResult;
import org.terifan.raccoon.blockdevice.BlockPointer;
import org.terifan.raccoon.util.Result;


public abstract class BTreeNode
{
	protected BlockPointer mBlockPointer;
	protected ArrayMap mMap;
	protected boolean mModified;
	protected int mLevel;
	protected boolean mHighlight;


	static class SplitResult
	{
		private final BTreeNode left;
		private final BTreeNode right;
		private final ArrayMapKey leftKey;
		private final ArrayMapKey rightKey;


		SplitResult(BTreeNode left, BTreeNode right, ArrayMapKey leftKey, ArrayMapKey rightKey)
		{
			this.left = left;
			this.right = right;
			this.leftKey = leftKey;
			this.rightKey = rightKey;
		}


		public BTreeNode left()
		{
			return left;
		}


		public ArrayMapKey leftKey()
		{
			return leftKey;
		}


		public BTreeNode right()
		{
			return right;
		}


		public ArrayMapKey rightKey()
		{
			return rightKey;
		}
	}


	enum RemoveResult
	{
		REMOVED,
		NO_MATCH
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


	enum VisitorState
	{
		CONTINUE,
		ABORT,
		SKIP
	}
}

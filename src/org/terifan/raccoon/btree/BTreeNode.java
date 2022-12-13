package org.terifan.raccoon.btree;

import org.terifan.raccoon.btree.ArrayMap.PutResult;
import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.Result;


abstract class BTreeNode
{
	final int mLevel;

	BlockPointer mBlockPointer;
	ArrayMap mMap;
	boolean mModified;


	static class SplitResult
	{
		private final BTreeNode left;
		private final BTreeNode right;
		private final MarshalledKey leftKey;
		private final MarshalledKey rightKey;
		SplitResult(BTreeNode left, BTreeNode right, MarshalledKey leftKey, MarshalledKey rightKey)
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


		public MarshalledKey leftKey()
		{
			return leftKey;
		}


		public BTreeNode right()
		{
			return right;
		}


		public MarshalledKey rightKey()
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


	abstract boolean get(BTree aImplementation, MarshalledKey aKey, ArrayMapEntry oEntry);


	abstract PutResult put(BTree aImplementation, MarshalledKey aKey, ArrayMapEntry aEntry, Result<ArrayMapEntry> oOldEntry);


	abstract RemoveResult remove(BTree aImplementation, MarshalledKey aKey, Result<ArrayMapEntry> oOldEntry);


	abstract SplitResult split(BTree aImplementation);


	abstract boolean commit(BTree aImplementation);


	protected abstract void postCommit();
}

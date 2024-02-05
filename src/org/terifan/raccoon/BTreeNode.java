package org.terifan.raccoon;

import org.terifan.raccoon.ArrayMap.PutResult;
import org.terifan.raccoon.blockdevice.BlockPointer;
import org.terifan.raccoon.document.Array;
import org.terifan.raccoon.util.Result;


public abstract class BTreeNode
{
	protected BTree mTree;
	protected BlockPointer mBlockPointer;
	protected boolean mModified;
	protected int mLevel;
	protected boolean mHighlight;


	protected BTreeNode(BTree aTree, int aLevel)
	{
		mTree = aTree;
		mLevel = aLevel;
	}


	abstract boolean get(ArrayMapKey aKey, ArrayMapEntry oEntry);


	abstract PutResult put(ArrayMapKey aKey, ArrayMapEntry aEntry, Result<ArrayMapEntry> oOldEntry);


	abstract RemoveResult remove(ArrayMapKey aKey, Result<ArrayMapEntry> oOldEntry);


	abstract void visit(BTreeVisitor aVisitor, ArrayMapKey aLowestKey, ArrayMapKey aHighestKey);


	abstract SplitResult split();


	abstract boolean commit();


	protected abstract void postCommit();


	protected abstract String integrityCheck();


	protected abstract int size();


	protected abstract void scan(ScanResult aScanResult);


	protected String stringifyKey(ArrayMapKey aKey)
	{
		Object keyValue = aKey.get();

		String value = "";

		if (keyValue instanceof Array)
		{
			for (Object k : (Array)keyValue)
			{
				if (!value.isEmpty())
				{
					value += ",";
				}
				value += k.toString().replaceAll("[^\\w]*", "");
			}
		}
		else
		{
			value += keyValue.toString().replaceAll("[^\\w]*", "");
		}

		return value;
	}


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
}

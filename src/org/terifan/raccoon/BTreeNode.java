package org.terifan.raccoon;

import org.terifan.raccoon.ArrayMap.InsertResult;
import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.Result;


abstract class BTreeNode
{
	final BTreeTableImplementation mImplementation;
	final BTreeIndex mParent;
	final long mNodeId;
	final int mLevel;

	long mGenerationId;
	BlockPointer mBlockPointer;
	ArrayMap mMap;
	boolean mModified;

	record SplitResult (BTreeNode left, BTreeNode right, MarshalledKey key) {}

	enum RemoveResult{
		OK,
		NONE,
		UPDATE_LOW
	}


	public BTreeNode(BTreeTableImplementation aImplementation, BTreeIndex aParent, int aLevel)
	{
		mImplementation = aImplementation;
		mNodeId = mImplementation.nextNodeIndex();
		mParent = aParent;
		mLevel = aLevel;
	}


	abstract boolean get(MarshalledKey aKey, ArrayMapEntry oEntry);


	abstract InsertResult put(MarshalledKey aKey, ArrayMapEntry aEntry, Result<ArrayMapEntry> oOldEntry);


	abstract RemoveResult remove(MarshalledKey aKey, Result<ArrayMapEntry> oOldEntry);


	abstract SplitResult split();


	abstract boolean commit();
}

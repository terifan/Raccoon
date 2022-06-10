package org.terifan.raccoon;

import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.Result;


abstract class BTreeNode
{
	final BTreeTableImplementation mImplementation;
	final long mNodeId;
	BlockPointer mBlockPointer;
	ArrayMap mMap;
	boolean mModified;
	BTreeIndex mParent;
	long mGenerationId;
	int mLevel;


	public BTreeNode(BTreeTableImplementation aImplementation, BTreeIndex aParent)
	{
		mImplementation = aImplementation;
		mNodeId = mImplementation.nextNodeIndex();
		mParent = aParent;
	}


	abstract boolean get(MarshalledKey aKey, ArrayMapEntry oEntry);


	abstract boolean put(MarshalledKey aKey, ArrayMapEntry aEntry, Result<ArrayMapEntry> oOldEntry);


	abstract boolean remove(MarshalledKey aKey, Result<ArrayMapEntry> oOldEntry);


	abstract Object[] split();


	abstract boolean commit();
}

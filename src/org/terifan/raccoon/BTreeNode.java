package org.terifan.raccoon;

import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.Result;


abstract class BTreeNode
{
	final BTreeTableImplementation mImplementation;
	BlockPointer mBlockPointer;
	ArrayMap mMap;
	boolean mModified;


	public BTreeNode(BTreeTableImplementation aImplementation)
	{
		mImplementation = aImplementation;
	}


	abstract boolean get(MarshalledKey aKey, ArrayMapEntry aEntry);


	abstract boolean put(BTreeIndex aParent, MarshalledKey aKey, ArrayMapEntry aEntry, Result<ArrayMapEntry> aResult);


	abstract BTreeNode[] split();


	abstract boolean commit();
}

package org.terifan.raccoon;

import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.Result;


abstract class BTreeNode
{
	BlockPointer mBlockPointer;
	boolean mChanged;
	ArrayMap mMap;


	abstract boolean put(BTreeIndex aParent, ArrayMapEntry aEntry, Result<ArrayMapEntry> aResult);


	abstract BTreeNode[] split();


	static BTreeNode newNode(BlockPointer aBlockPointer)
	{
		return aBlockPointer.getBlockType() == BlockType.INDEX ? new BTreeIndex() : new BTreeLeaf();
	}
}

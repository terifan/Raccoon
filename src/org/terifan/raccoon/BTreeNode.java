package org.terifan.raccoon;

import org.terifan.raccoon.storage.BlockPointer;


abstract class BTreeNode
{
	BlockPointer mBlockPointer;
	boolean mChanged;
	ArrayMap mMap;


	static BTreeNode newNode(BlockPointer aBlockPointer)
	{
		return aBlockPointer.getBlockType() == BlockType.INDEX ? new BTreeIndex() : new BTreeLeaf();
	}
}

package org.terifan.raccoon;

import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.storage.IBlockAccessor;


abstract class HashTableAbstractNode
{
	protected final HashTable mHashTable;
	protected final IBlockAccessor mBlockAccessor;
	protected final HashTableNode mParent;
	protected BlockPointer mBlockPointer;
	protected FlushAction mFlushAction;

	enum FlushAction
	{
		DO_NOTHING,
		WRITE,
		DELETE
	}

	HashTableAbstractNode(HashTable aHashTable, IBlockAccessor aBlockAccessor, HashTableNode aParent, BlockPointer aBlockPointer)
	{
		mHashTable = aHashTable;
		mBlockAccessor = aBlockAccessor;
		mBlockPointer = aBlockPointer;
		mParent = aParent;
	}


	HashTableNode getParent()
	{
		return mParent;
	}


	abstract byte[] array();


	abstract BlockType getBlockType();


	abstract void freeBlock();


	abstract void writeBlock();


	@Deprecated
	abstract void writeBlock(int aRangeOffset, int aRangeSize, int aLevel);


	abstract void flush();


	abstract String integrityCheck();


	@Deprecated
	abstract boolean remove(ArrayMapEntry aEntry);


	BlockPointer getBlockPointer()
	{
		return mBlockPointer;
	}


	void setBlockPointer(BlockPointer aBlockPointer)
	{
		mBlockPointer = aBlockPointer;
	}
}
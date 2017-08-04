package org.terifan.raccoon;

import org.terifan.raccoon.storage.BlockPointer;


abstract class Node
{
	protected final HashTable mHashTable;
	protected final HashTableNode mParent;
	protected BlockPointer mBlockPointer;


	Node(HashTable aHashTable, HashTableNode aParent, BlockPointer aBlockPointer)
	{
		mHashTable = aHashTable;
		mBlockPointer = aBlockPointer;
		mParent = aParent;
	}


	abstract byte[] array();

	abstract BlockType getBlockType();

	abstract boolean get(ArrayMapEntry aEntry, int aLevel);

	abstract boolean put(ArrayMapEntry aEntry, int aLevel);

	abstract boolean remove(ArrayMapEntry aEntry, int aLevel);

	abstract String integrityCheck();


	BlockPointer writeBlock()
	{
		return writeBlock(mBlockPointer.getRangeOffset(), mBlockPointer.getRangeSize(), mBlockPointer.getLevel());
	}


	HashTableNode getParent()
	{
		return mParent;
	}


	BlockPointer getBlockPointer()
	{
		return mBlockPointer;
	}


	void setBlockPointer(BlockPointer aBlockPointer)
	{
		this.mBlockPointer = aBlockPointer;
	}


	BlockPointer writeBlock(int aRangeOffset, int aRangeSize, int aLevel)
	{
		mBlockPointer = mHashTable.getBlockAccessor().writeBlock(array(), 0, array().length, mHashTable.getTransactionId().get(), getBlockType(), aRangeOffset, aRangeSize, aLevel);

//		System.out.println("write " + mBlockPointer);

		assert mBlockPointer.getLevel() != 0 || mBlockPointer.getRangeOffset() == 0 && mBlockPointer.getRangeSize() == mHashTable.getNodeSize() / BlockPointer.SIZE : "Illegal pointer: " + mBlockPointer;

		return mBlockPointer;
	}


	void freeBlock()
	{
//		System.out.println("free  " + mBlockPointer);

		mHashTable.getBlockAccessor().freeBlock(mBlockPointer);
	}
}

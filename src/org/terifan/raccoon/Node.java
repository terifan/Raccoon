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


	HashTableNode getParent()
	{
		return mParent;
	}


	abstract byte[] array();


	abstract BlockType getBlockType();


	abstract void freeBlock();


	abstract void writeBlock();


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
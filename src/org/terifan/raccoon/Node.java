package org.terifan.raccoon;

import org.terifan.raccoon.storage.BlockPointer;

public abstract class Node
{
	protected final HashTable mHashTable;
	protected final HashTableNode mParent;
	protected BlockPointer mBlockPointer;


	public Node(HashTable aHashTable, HashTableNode aParent, BlockPointer aBlockPointer)
	{
		mHashTable = aHashTable;
		mBlockPointer = aBlockPointer;
		mParent = aParent;
	}

	
	abstract byte[] array();

	abstract BlockType getBlockType();
	
	abstract boolean get(ArrayMapEntry aEntry, int aLevel);


	abstract boolean put(ArrayMapEntry aEntry, int aLevel);


	abstract BlockPointer writeBlock(int aRange);


	abstract void freeBlock();


	abstract boolean remove(ArrayMapEntry aEntry, int aLevel);


	abstract String integrityCheck();
}

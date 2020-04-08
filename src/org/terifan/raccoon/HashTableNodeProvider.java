package org.terifan.raccoon;

import org.terifan.raccoon.storage.BlockPointer;


class HashTableNodeProvider
{
	private HashTable mHashTable;


	public HashTableNodeProvider(HashTable aHashTable)
	{
		mHashTable = aHashTable;
	}


	<T extends Node> T read(BlockPointer aBlockPointer)
	{
		if (aBlockPointer.getBlockType() == BlockType.INDEX)
		{
			return (T)new HashTableNode(mHashTable, aBlockPointer);
		}
		if (aBlockPointer.getBlockType() == BlockType.LEAF)
		{
			return (T)new HashTableLeaf(mHashTable, aBlockPointer);
		}
		throw new IllegalArgumentException();
	}
}

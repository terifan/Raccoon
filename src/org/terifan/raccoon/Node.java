package org.terifan.raccoon;

import java.util.HashMap;
import org.terifan.raccoon.storage.BlockPointer;


abstract class Node
{
	protected final HashTable mHashTable;
	protected final HashTableNode mParent;
	protected BlockPointer mBlockPointer;
	HashMap<Integer,Node> mChildren = new HashMap<>();


	Node(HashTable aHashTable, HashTableNode aParent, BlockPointer aBlockPointer)
	{
		mHashTable = aHashTable;
		mBlockPointer = aBlockPointer;
		mParent = aParent;
	}


	abstract byte[] array();

	abstract BlockType getBlockType();

	@Deprecated
	abstract boolean remove(ArrayMapEntry aEntry);

	abstract String integrityCheck();


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
		mBlockPointer = aBlockPointer;
	}


	Node readBlock(BlockPointer aBlockPointer)
	{
		System.out.println("read  " + aBlockPointer);
		
		Node node = mChildren.get(aBlockPointer.getRangeOffset());
		
		if (node == null)
		{
			node = mHashTable.read(aBlockPointer, (HashTableNode)this);
			
			mChildren.put(aBlockPointer.getRangeOffset(), node);
		}

		return node;
	}


	void writeBlock()
	{
		writeBlock(mBlockPointer.getRangeOffset(), mBlockPointer.getRangeSize(), mBlockPointer.getLevel());
	}


	void writeBlock(int aRangeOffset, int aRangeSize, int aLevel)
	{
		assert aLevel != 0 || aRangeOffset == 0 && aRangeSize * BlockPointer.SIZE == mHashTable.getNodeSize();

		mBlockPointer = new BlockPointer();
		mBlockPointer.setRangeOffset(aRangeOffset);
		mBlockPointer.setRangeSize(aRangeSize);
		mBlockPointer.setLevel(aLevel);
		
//		mBlockPointer = mHashTable.getBlockAccessor().writeBlock(array(), 0, array().length, mHashTable.getTransactionId().get(), getBlockType(), aRangeOffset, aRangeSize, aLevel);

		if (mParent != null)
		{
			mParent.mChildren.put(aRangeOffset, this);
			mParent.setPointer(mBlockPointer);
		}

		System.out.println("write " + mBlockPointer);
	}


	void freeBlock()
	{
		if (mBlockPointer.getBlockType() != BlockType.FREE)
		{
			System.out.println("free  " + mBlockPointer);

			mHashTable.getBlockAccessor().freeBlock(mBlockPointer);

			mBlockPointer.setBlockType(BlockType.FREE);

			mChildren.remove(mBlockPointer.getRangeOffset());
		}
	}


	void flush()
	{
		for (Node node : mChildren.values())
		{
			node.flush();
		}
		
		mBlockPointer = mHashTable.getBlockAccessor().writeBlock(array(), 0, array().length, mHashTable.getTransactionId(), getBlockType(), mBlockPointer.getRangeOffset(), mBlockPointer.getRangeSize(), mBlockPointer.getLevel());

		if (mParent != null)
		{
			mParent.mChildren.put(mBlockPointer.getRangeOffset(), this);
			mParent.setPointer(mBlockPointer);
		}
	}
}

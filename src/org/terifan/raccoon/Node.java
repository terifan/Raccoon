package org.terifan.raccoon;

import org.terifan.raccoon.storage.BlockPointer;


abstract class Node
{
	protected final HashTable mHashTable;
	protected final HashTableNode mParent;
	protected BlockPointer mBlockPointer;
	private boolean mFree;


	Node(HashTable aHashTable, HashTableNode aParent, BlockPointer aBlockPointer)
	{
		mHashTable = aHashTable;
		mBlockPointer = aBlockPointer;
		mParent = aParent;

		mFree = mBlockPointer == null;
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

		return mHashTable.read(aBlockPointer, (HashTableNode)this);
	}


	BlockPointer writeBlock()
	{
		return writeBlock(mBlockPointer.getRangeOffset(), mBlockPointer.getRangeSize(), mBlockPointer.getLevel());
	}


	BlockPointer writeBlock(int aRangeOffset, int aRangeSize, int aLevel)
	{
		assert aLevel != 0 || aRangeOffset == 0 && aRangeSize * BlockPointer.SIZE == mHashTable.getNodeSize();

		if (!mFree && mBlockPointer != null)
		{
			freeBlock();
		}

		mBlockPointer = mHashTable.getBlockAccessor().writeBlock(array(), 0, array().length, mHashTable.getTransactionId().get(), getBlockType(), aRangeOffset, aRangeSize, aLevel);
		mFree = false;

		System.out.println("write " + mBlockPointer);

		if (mParent != null)
		{
			mParent.setPointer(mBlockPointer);
		}

		return mBlockPointer;
	}


	void freeBlock()
	{
		if (!mFree)
		{
			System.out.println("free  " + mBlockPointer);

			mHashTable.getBlockAccessor().freeBlock(mBlockPointer);

			mFree = true;
		}
	}
}

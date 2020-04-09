package org.terifan.raccoon;

import org.terifan.raccoon.storage.BlockPointer;
import java.util.ArrayDeque;
import java.util.ConcurrentModificationException;
import java.util.Iterator;


final class HashTableNodeIterator implements Iterator<ArrayMapEntry>
{
	private long mModCount;
	private ArrayDeque<BlockPointer> mNodes;
	private Iterator<ArrayMapEntry> mMap;
	private ArrayMapEntry mNextEntry;
	private HashTable mHashTable;
	private boolean mHasEntry;


	HashTableNodeIterator(HashTable aHashTable, BlockPointer aBlockPointer)
	{
		mNodes = new ArrayDeque<>();

		mHashTable = aHashTable;
		mModCount = mHashTable.mModCount;

		mNodes.add(aBlockPointer);
	}


	HashTableNodeIterator(HashTable aHashTable, ArrayMap aDataPage)
	{
		mNodes = new ArrayDeque<>();

		mHashTable = aHashTable;
		mModCount = mHashTable.mModCount;

		mMap = aDataPage.iterator();
	}


	@Override
	public boolean hasNext()
	{
		if (mHasEntry)
		{
			return true;
		}

		if (mMap != null)
		{
			if (mMap.hasNext())
			{
				mNextEntry = mMap.next();
				mHasEntry = true;

				return true;
			}

			mMap = null;
		}

		if (mNodes.isEmpty())
		{
			return false;
		}

		BlockPointer pointer = mNodes.pop();

		if (pointer.getBlockType() == BlockType.LEAF)
		{
			HashTableLeaf leaf = new HashTableLeaf(mHashTable, null, pointer);

			mMap = leaf.iterator();

			if (!mMap.hasNext()) // should never happend
			{
				mMap = null;
			}

			return hasNext();
		}

		HashTableNode node = new HashTableNode(mHashTable, pointer);

		for (int i = 0; i < mHashTable.mPointersPerNode; i++)
		{
			BlockPointer next = node.getPointer(i);

			if (next != null && (next.getBlockType() == BlockType.INDEX || next.getBlockType() == BlockType.LEAF))
			{
				mNodes.addLast(next);
			}
		}

		return hasNext();
	}


	@Override
	public ArrayMapEntry next()
	{
		if (mModCount != mHashTable.mModCount)
		{
			throw new ConcurrentModificationException();
		}
		if (!mHasEntry)
		{
			if (!hasNext())
			{
				throw new IllegalStateException();
			}
		}

		mHasEntry = false;

		if (!mMap.hasNext())
		{
			mMap = null;
		}

		return mNextEntry;
	}


	@Override
	public void remove()
	{
		throw new UnsupportedOperationException("Not supported.");
	}
}

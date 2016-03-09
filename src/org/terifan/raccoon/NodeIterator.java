package org.terifan.raccoon;

import org.terifan.raccoon.io.BlockPointer;
import java.util.ArrayDeque;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import org.terifan.raccoon.io.BlockPointer.BlockType;


class NodeIterator implements Iterator<Entry>
{
	private long mModCount;
	private int mEntryIndex;
	private ArrayDeque<BlockPointer> mNodes;
	private LeafNode mMap;
	private Entry mNextEntry;
	private HashTable mHashTable;
	private boolean mHasEntry;


	NodeIterator(HashTable aHashTable, BlockPointer aBlockPointer)
	{
		mNodes = new ArrayDeque<>();

		mHashTable = aHashTable;
		mNextEntry = new Entry(aHashTable);
		mModCount = mHashTable.mModCount;

		mNodes.add(aBlockPointer);
	}


	NodeIterator(HashTable aHashTable, LeafNode aDataPage)
	{
		mNodes = new ArrayDeque<>();

		mHashTable = aHashTable;
		mNextEntry = new Entry(aHashTable);
		mModCount = mHashTable.mModCount;

		mMap = aDataPage;
	}


	@Override
	public boolean hasNext()
	{
		if (mModCount != mHashTable.mModCount)
		{
			throw new ConcurrentModificationException();
		}

		if (mHasEntry)
		{
			return true;
		}

		if (mMap != null)
		{
			byte[] key = mMap.getKey(mEntryIndex);
			byte[] value = mMap.get(key);

			mNextEntry.setKey(key);
			mNextEntry.setValue(value);
			mHasEntry = true;

			return true;
		}

		if (mNodes.isEmpty())
		{
			return false;
		}

		BlockPointer pointer = mNodes.pop();

		if (pointer.getType() == BlockType.NODE_LEAF)
		{
			mMap = mHashTable.readLeaf(pointer);

			if (mMap.isEmpty()) // should never happend
			{
				mMap = null;
			}

			return hasNext();
		}

		IndexNode node = mHashTable.readNode(pointer);

		for (int i = 0; i < node.getPointerCount(); i++)
		{
			BlockPointer next = node.getPointer(i);

			if (next != null && (next.getType() == BlockType.NODE_INDX || next.getType() == BlockType.NODE_LEAF))
			{
				mNodes.addLast(next);
			}
		}

		return hasNext();
	}


	@Override
	public Entry next()
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

		mEntryIndex++;
		mHasEntry = false;

		if (mMap.size() == mEntryIndex)
		{
			mMap = null;
			mEntryIndex = 0;
		}

		return mNextEntry;
	}


	@Override
	public void remove()
	{
		throw new UnsupportedOperationException("Not supported.");
	}
}

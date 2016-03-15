package org.terifan.raccoon;

import org.terifan.raccoon.io.BlockPointer;
import java.util.ArrayDeque;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import org.terifan.raccoon.io.BlockPointer.BlockType;
import org.terifan.raccoon.util.Log;


class NodeIterator implements Iterator<LeafEntry>
{
	private long mModCount;
	private int mEntryIndex;
	private ArrayDeque<BlockPointer> mNodes;
	private Iterator<LeafEntry> mMap;
	private LeafEntry mNextEntry;
	private HashTable mHashTable;
	private boolean mHasEntry;


	NodeIterator(HashTable aHashTable, BlockPointer aBlockPointer)
	{
		mNodes = new ArrayDeque<>();

		mHashTable = aHashTable;
		mNextEntry = new LeafEntry();
		mModCount = mHashTable.mModCount;

		mNodes.add(aBlockPointer);
	}


	NodeIterator(HashTable aHashTable, LeafNode aDataPage)
	{
		mNodes = new ArrayDeque<>();

		mHashTable = aHashTable;
		mNextEntry = new LeafEntry();
		mModCount = mHashTable.mModCount;

		mMap = aDataPage.iterator();
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
			if (mMap.hasNext())
			{
				LeafEntry entry = mMap.next();

				mNextEntry.setKey(entry.getKey());
				mNextEntry.setValue(entry.getValue());
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

		if (pointer.getType() == BlockType.NODE_LEAF)
		{
			mMap = mHashTable.readLeaf(pointer).iterator();

			if (!mMap.hasNext()) // should never happend
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
	public LeafEntry next()
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

		if (!mMap.hasNext())
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

package org.terifan.raccoon.hashtable;

import org.terifan.raccoon.io.BlockPointer;
import org.terifan.raccoon.Entry;
import org.terifan.raccoon.LeafNode;
import java.util.ArrayDeque;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import static org.terifan.raccoon.Node.*;
import org.terifan.raccoon.util.Result;


public class NodeIterator implements Iterator<Entry>
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
		mNextEntry = new Entry();
		mModCount = mHashTable.mModCount;

		mNodes.add(aBlockPointer);
	}


	NodeIterator(HashTable aHashTable, LeafNode aDataPage)
	{
		mNodes = new ArrayDeque<>();

		mHashTable = aHashTable;
		mNextEntry = new Entry();
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
			Result<Integer> type = new Result<>();

			byte[] key = mMap.getKey(mEntryIndex);

			mNextEntry.setKey(key);
			mNextEntry.setValue(mMap.get(key, type));
			mNextEntry.setType(type.get());
			mHasEntry = true;

			return true;
		}

		if (mNodes.isEmpty())
		{
			return false;
		}

		BlockPointer pointer = mNodes.pop();

		if (pointer.getType() == LEAF)
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

			if (next != null && (next.getType() == NODE || next.getType() == LEAF))
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

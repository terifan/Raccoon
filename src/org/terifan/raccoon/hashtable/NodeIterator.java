package org.terifan.raccoon.hashtable;

import org.terifan.raccoon.storage.BlockPointer;
import java.util.ArrayDeque;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import org.terifan.raccoon.ArrayMap;
import org.terifan.raccoon.RecordEntry;
import org.terifan.raccoon.BlockType;


final class NodeIterator implements Iterator<RecordEntry>
{
	private long mModCount;
	private ArrayDeque<BlockPointer> mNodes;
	private Iterator<RecordEntry> mMap;
	private RecordEntry mNextEntry;
	private HashTable mHashTable;
	private boolean mHasEntry;


	NodeIterator(HashTable aHashTable, BlockPointer aBlockPointer)
	{
		mNodes = new ArrayDeque<>();

		mHashTable = aHashTable;
		mModCount = mHashTable.mModCount;

		mNodes.add(aBlockPointer);
	}


	NodeIterator(HashTable aHashTable, ArrayMap aDataPage)
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
			LeafNode leaf = mHashTable.readLeaf(pointer);

			mMap = leaf.iterator();

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

			if (next != null && (next.getBlockType() == BlockType.INDEX || next.getBlockType() == BlockType.LEAF))
			{
				mNodes.addLast(next);
			}
		}
		
		node.gc();

		return hasNext();
	}


	@Override
	public RecordEntry next()
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

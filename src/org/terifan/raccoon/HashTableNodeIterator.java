package org.terifan.raccoon;

import java.util.ArrayDeque;
import java.util.ConcurrentModificationException;
import java.util.Iterator;


final class HashTableNodeIterator implements Iterator<ArrayMapEntry>
{
	private final long mModCount;
	private ArrayDeque<Iterator<HashTableNode>> mQueue;
	private Iterator<ArrayMapEntry> mValueIt;
	private Iterator<HashTableNode> mNodeIt;
	private ArrayMapEntry mNextEntry;
	private HashTable mHashTable;


	HashTableNodeIterator(HashTable aHashTable, HashTableNode aNode)
	{
		mHashTable = aHashTable;
		mModCount = mHashTable.mModCount;

		if (aNode instanceof HashTableLeafNode)
		{
			mValueIt = ((HashTableLeafNode)aNode).iterator();
		}
		else
		{
			mQueue = new ArrayDeque<>();
			mNodeIt = ((HashTableInnerNode)aNode).iterator();
		}
	}


	@Override
	public boolean hasNext()
	{
		for (;;)
		{
			if (mNextEntry != null)
			{
				return true;
			}

			if (mValueIt != null)
			{
				if (mValueIt.hasNext())
				{
					mNextEntry = mValueIt.next();

					return true;
				}

				mValueIt = null;
			}

			if (mNodeIt == null)
			{
				return false;
			}

			if (mNodeIt.hasNext())
			{
				HashTableNode node = mNodeIt.next();

				if (node instanceof HashTableLeafNode)
				{
					mValueIt = ((HashTableLeafNode)node).iterator();
				}
				else if (node instanceof HashTableInnerNode)
				{
					mQueue.addFirst(mNodeIt);
					mNodeIt = ((HashTableInnerNode)node).iterator();
				}

				return hasNext();
			}

			if (mQueue.isEmpty())
			{
				return false;
			}

			mNodeIt = mQueue.pop();
		}
	}


	@Override
	public ArrayMapEntry next()
	{
		if (mModCount != mHashTable.mModCount)
		{
			throw new ConcurrentModificationException();
		}
		if (mNextEntry == null)
		{
			throw new IllegalStateException();
		}

		ArrayMapEntry tmp = mNextEntry;

		mNextEntry = null;

		return tmp;
	}


	@Override
	public void remove()
	{
		throw new UnsupportedOperationException("Not supported.");
	}
}

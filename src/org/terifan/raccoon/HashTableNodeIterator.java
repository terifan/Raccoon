package org.terifan.raccoon;

import java.util.ArrayDeque;
import java.util.ConcurrentModificationException;
import java.util.Iterator;


final class HashTableNodeIterator implements Iterator<ArrayMapEntry>
{
	private final long mModCount;
	private ArrayDeque<Iterator<Node>> mQueue;
	private Iterator<ArrayMapEntry> mValueIt;
	private Iterator<Node> mNodeIt;
	private ArrayMapEntry mNextEntry;
	private HashTable mHashTable;


	HashTableNodeIterator(HashTable aHashTable, Node aNode)
	{
		mHashTable = aHashTable;
		mModCount = mHashTable.mModCount;

		if (aNode instanceof HashTableLeaf)
		{
			mValueIt = ((HashTableLeaf)aNode).iterator();
		}
		else
		{
			mQueue = new ArrayDeque<>();
			mNodeIt = ((HashTableNode)aNode).iterator();
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
				Node node = mNodeIt.next();

				if (node instanceof HashTableLeaf)
				{
					mValueIt = ((HashTableLeaf)node).iterator();
				}
				else if (node instanceof HashTableNode)
				{
					mQueue.addFirst(mNodeIt);
					mNodeIt = ((HashTableNode)node).iterator();
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

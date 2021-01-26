package org.terifan.raccoon;

import java.util.ArrayDeque;
import java.util.ConcurrentModificationException;
import java.util.Iterator;


final class HashTreeTableNodeIterator implements Iterator<ArrayMapEntry>
{
	private final long mModCount;
	private ArrayDeque<Iterator<HashTreeTableNode>> mQueue;
	private Iterator<ArrayMapEntry> mValueIt;
	private Iterator<HashTreeTableNode> mNodeIt;
	private ArrayMapEntry mNextEntry;
	private HashTreeTable mHashTable;


	HashTreeTableNodeIterator(HashTreeTable aHashTable, HashTreeTableNode aNode)
	{
		mHashTable = aHashTable;
		mModCount = mHashTable.mModCount;

		if (aNode instanceof HashTreeTableLeafNode)
		{
			mValueIt = ((HashTreeTableLeafNode)aNode).iterator();
		}
		else
		{
			mQueue = new ArrayDeque<>();
			mNodeIt = ((HashTreeTableInnerNode)aNode).iterator();
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
				HashTreeTableNode node = mNodeIt.next();

				if (node instanceof HashTreeTableLeafNode)
				{
					mValueIt = ((HashTreeTableLeafNode)node).iterator();
				}
				else if (node instanceof HashTreeTableInnerNode)
				{
					mQueue.addFirst(mNodeIt);
					mNodeIt = ((HashTreeTableInnerNode)node).iterator();
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

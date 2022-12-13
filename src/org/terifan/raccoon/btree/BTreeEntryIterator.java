package org.terifan.raccoon.btree;

import java.util.Iterator;


public class BTreeEntryIterator implements Iterator<ArrayMapEntry>
{
	private BTreeNodeIterator mNodeIterator;
	private Iterator<ArrayMapEntry> mElements;


	BTreeEntryIterator(BTree aTree, BTreeNode aRoot)
	{
		mNodeIterator = new BTreeNodeIterator(aTree, aRoot);
	}


	@Override
	public boolean hasNext()
	{
		if (mElements == null)
		{
			if (!mNodeIterator.hasNext())
			{
				return false;
			}

			mElements = mNodeIterator.next().mMap.iterator();
		}

		if (!mElements.hasNext())
		{
			mElements = null;
			return hasNext();
		}

		return true;
	}


	@Override
	public ArrayMapEntry next()
	{
		return mElements.next();
	}
}

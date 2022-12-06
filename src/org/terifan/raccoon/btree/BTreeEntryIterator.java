package org.terifan.raccoon.btree;

import java.util.Iterator;


public class BTreeEntryIterator implements Iterator<ArrayMapEntry>
{
	private BTreeNodeIterator mIterator;
	private Iterator<ArrayMapEntry> mElements;


	BTreeEntryIterator(BTreeNode aRoot)
	{
		mIterator = new BTreeNodeIterator(aRoot);
	}


	@Override
	public boolean hasNext()
	{
		if (mElements == null)
		{
			if (!mIterator.hasNext())
			{
				return false;
			}

			mElements = mIterator.next().mMap.iterator();
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

package org.terifan.raccoon.btree;

import java.util.Iterator;


public class BTreeEntryIterator implements Iterator<ArrayMapEntry>
{
	private BTreeNodeIterator mNodeIterator;
	private Iterator<ArrayMapEntry> mMapIterator;


	BTreeEntryIterator(BTree aTree)
	{
		mNodeIterator = new BTreeNodeIterator(aTree, aTree.getRoot());
	}


	@Override
	public boolean hasNext()
	{
		if (mMapIterator == null)
		{
			if (!mNodeIterator.hasNext())
			{
				return false;
			}

			mMapIterator = mNodeIterator.next().mMap.iterator();
		}

		if (!mMapIterator.hasNext())
		{
			mMapIterator = null;
			return hasNext();
		}

		return true;
	}


	@Override
	public ArrayMapEntry next()
	{
		return mMapIterator.next();
	}
}

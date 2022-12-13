package org.terifan.raccoon.btree;

import java.util.Iterator;
import org.terifan.raccoon.btree.ArrayMap.MapEntryIterator;


public class BTreeEntryIterator implements Iterator<ArrayMapEntry>
{
	private BTreeNodeIterator mNodeIterator;
	private MapEntryIterator mMapIterator;


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

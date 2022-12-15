package org.terifan.raccoon;

import java.util.Iterator;
import org.terifan.raccoon.ArrayMap.MapEntryIterator;


public class BTreeEntryIterator implements Iterator<ArrayMapEntry>
{
	private BTreeNodeIterator mNodeIterator;
	private MapEntryIterator mMapIterator;


	public BTreeEntryIterator(BTree aTree)
	{
		mNodeIterator = new BTreeNodeIterator(aTree);
	}


	@Override
	public boolean hasNext()
	{
		if (mMapIterator == null)
		{
			if (!mNodeIterator.hasNext())
			{
				mMapIterator = null;
				mNodeIterator = null;
				return false;
			}

			mMapIterator = mNodeIterator.next().mMap.iterator();
		}

		if (!mMapIterator.hasNext())
		{
			mMapIterator = null;

			boolean hasNext = hasNext();

			if (!hasNext)
			{
				mMapIterator = null;
				mNodeIterator = null;
			}

			return hasNext;
		}

		return true;
	}


	@Override
	public ArrayMapEntry next()
	{
		return mMapIterator.next();
	}
}

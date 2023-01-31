package org.terifan.raccoon;

import java.util.Iterator;
import org.terifan.raccoon.ArrayMap.ArrayMapEntryIterator;


public class BTreeEntryIterator implements Iterator<ArrayMapEntry>
{
	private BTreeNodeIterator mBTreeNodeIterator;
	private ArrayMapEntryIterator mArrayMapEntryIterator;
	private ArrayMapKey mRangeLow;
	private ArrayMapKey mRangeHigh;


	public BTreeEntryIterator(BTree aTree)
	{
		mBTreeNodeIterator = new BTreeNodeIterator(aTree);
	}


	public void setRange(ArrayMapKey aLow, ArrayMapKey aHigh)
	{
		mRangeLow = aLow;
		mRangeHigh = aHigh;
		mBTreeNodeIterator.setRange(aLow, aHigh);
	}


	@Override
	public boolean hasNext()
	{
		if (mArrayMapEntryIterator == null)
		{
			if (!mBTreeNodeIterator.hasNext())
			{
				mArrayMapEntryIterator = null;
				mBTreeNodeIterator = null;
				return false;
			}

			mArrayMapEntryIterator = mBTreeNodeIterator.next().mMap.iterator();
			mArrayMapEntryIterator.setRange(mRangeLow, mRangeHigh);
		}

		if (!mArrayMapEntryIterator.hasNext())
		{
			mArrayMapEntryIterator = null;

			boolean hasNext = hasNext();

			if (!hasNext)
			{
				mArrayMapEntryIterator = null;
				mBTreeNodeIterator = null;
			}

			return hasNext;
		}

		return true;
	}


	@Override
	public ArrayMapEntry next()
	{
		return mArrayMapEntryIterator.next();
	}
}

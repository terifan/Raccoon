package org.terifan.raccoon;


public class ArrayMapEntryIterator extends Sequence<ArrayMapEntry>
{
	private ArrayMap mMap;
	private Query mQuery;
	private int mIndex;


	public ArrayMapEntryIterator(ArrayMap aMap, Query aQuery)
	{
		mMap = aMap;
		mQuery = aQuery;
	}


	@Override
	public ArrayMapEntry advance()
	{
		ArrayMapEntry pending = new ArrayMapEntry();

		for (; mIndex < mMap.mEntryCount; mIndex++)
		{
			mMap.loadKey(mIndex, pending);

			if ((mQuery.mRangeLow == null || pending.getKey().compareTo(mQuery.mRangeLow) >= 0) && (mQuery.mRangeHigh == null || pending.getKey().compareTo(mQuery.mRangeHigh) <= 0))
			{
				mMap.loadValue(mIndex++, pending);
				return pending;
			}
		}

		return null;
	}
}

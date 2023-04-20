package org.terifan.raccoon;

import java.util.Iterator;


public class ArrayMapEntryIterator implements Iterator<ArrayMapEntry>
{
	private ArrayMap mMap;
	private int mIndex;


	public ArrayMapEntryIterator(ArrayMap aMap)
	{
		mMap = aMap;
	}


	@Override
	public boolean hasNext()
	{
		return mIndex < mMap.size();
	}


	@Override
	public ArrayMapEntry next()
	{
		return mMap.loadKeyAndValue(mIndex++, new ArrayMapEntry());
	}
}

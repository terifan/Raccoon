package org.terifan.raccoon.storage;

import java.util.HashMap;
import java.util.LinkedList;


class Cache<K,V>
{
	private final HashMap<K,V> mCache;
	private final LinkedList<K> mCacheOrder;
	private final int mCapacity;


	public Cache(int aCapacity)
	{
		mCache = new HashMap<>();
		mCacheOrder = new LinkedList<>();
		mCapacity = aCapacity;
	}


	public V get(K aKey)
	{
		return mCache.get(aKey);
	}


	public void put(K aKey, V aBuffer)
	{
		mCache.put(aKey, aBuffer);

		mCacheOrder.remove(aKey);
		mCacheOrder.addFirst(aKey);

		if (mCache.size() > mCapacity)
		{
			mCache.remove(mCacheOrder.removeLast());
		}
	}


	public void remove(K aKey)
	{
		mCache.remove(aKey);
		mCacheOrder.remove(aKey);
	}


	public void clear()
	{
		mCache.clear();
		mCacheOrder.clear();
	}
}

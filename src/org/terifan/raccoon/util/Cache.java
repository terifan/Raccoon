package org.terifan.raccoon.util;

import java.util.LinkedHashMap;
import java.util.Map.Entry;


public final class Cache<K, V> extends LinkedHashMap<K, V>
{
	private static final long serialVersionUID = 1L;
	private final int mCapacity;


	public Cache(int aCapacity)
	{
		super(16, 0.75f, true);

		mCapacity = aCapacity;
	}


	@Override
	protected boolean removeEldestEntry(Entry<K, V> aEldest)
	{
		return size() > mCapacity;
	}
}

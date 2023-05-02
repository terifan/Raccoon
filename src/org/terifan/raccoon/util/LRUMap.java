package org.terifan.raccoon.util;

import java.util.LinkedHashMap;
import java.util.Map;


public class LRUMap<K, V>
{
	private LinkedHashMap<K, V> mMap;


	public LRUMap(int aCapacity)
	{
		mMap = new LinkedHashMap<>(aCapacity + 1, 0.75f, true)
		{
			@Override
			protected boolean removeEldestEntry(Map.Entry<K, V> aEldest)
			{
				return size() > aCapacity;
			}
		};
	}


	public synchronized V put(K aKey, V aValue)
	{
		return mMap.put(aKey, aValue);
	}


	public synchronized V get(K aKey)
	{
		return mMap.get(aKey);
	}


	public synchronized boolean containsKey(K aKey)
	{
		return mMap.containsKey(aKey);
	}


	public synchronized V remove(K aKey)
	{
		return mMap.remove(aKey);
	}


	public synchronized int size()
	{
		return mMap.size();
	}


	public synchronized void clear()
	{
		mMap.clear();
	}


	public static void main(String ... args)
	{
		try
		{
			LRUMap<String, Integer> cache = new LRUMap<>(3);

			cache.put("a", 1);
			cache.put("b", 2);
			cache.put("c", 3);
			cache.put("d", 4);
			cache.put("e", 5);
			cache.put("f", 6);

			System.out.println(cache.get("a"));
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}
}

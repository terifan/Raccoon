package org.terifan.raccoon.util;

import java.util.Collection;
import java.util.HashMap;


public class DualMap<S, T, U>
{
	private final HashMap<T, U> EMPTY = new HashMap<>();
	private HashMap<S, HashMap<T, U>> mMap;


	public DualMap()
	{
		mMap = new HashMap<>();
	}


	public void put(S aFirst, T aSecond, U aValue)
	{
		HashMap<T, U> map = mMap.computeIfAbsent(aFirst, k -> new HashMap<>());
		map.put(aSecond, aValue);
	}


	public HashMap<T, U> get(S aFirst)
	{
		return mMap.get(aFirst);
	}


	public Collection<HashMap<T, U>> values()
	{
		return mMap.values();
	}


	public Iterable<U> values(S aFirst)
	{
		return mMap.getOrDefault(aFirst, EMPTY).values();
	}


	@Override
	public String toString()
	{
		return mMap.toString();
	}


	public static void main(String... args)
	{
		try
		{
			DualMap<String, String, Integer> map = new DualMap<>();
			map.put("a", "b", 0);
			map.put("a", "c", 1);
			map.put("g", "a", 2);
			System.out.println(map);
			System.out.println(map.get("a"));
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}
}

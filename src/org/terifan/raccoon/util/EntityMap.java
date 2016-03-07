package org.terifan.raccoon.util;

import java.util.HashMap;
import java.util.Map;


public class EntityMap
{
	private final Map<String, Object> mValues;


	public EntityMap()
	{
		mValues = new HashMap<>();
	}


	public boolean containsKey(String aKey)
	{
		return mValues.containsKey(aKey);
	}


	public Object get(String aKey)
	{
		return mValues.get(aKey);
	}


	public EntityMap put(String aKey, Object aValue)
	{
		mValues.put(aKey, aValue);
		return this;
	}
}

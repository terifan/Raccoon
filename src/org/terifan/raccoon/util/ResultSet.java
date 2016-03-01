package org.terifan.raccoon.util;

import java.util.HashMap;
import java.util.Map;
import org.terifan.raccoon.serialization.FieldType;


public class ResultSet
{
	private final static long serialVersionUID = 1L;

	private final Map<Integer, Object> mValues;
	private final Map<String, FieldType> mTypes;


	public ResultSet()
	{
		mValues = new HashMap<>();
		mTypes = new HashMap<>();
	}


	public boolean containsKey(String aKey)
	{
		return mTypes.containsKey(aKey);
	}


	public Object get(String aKey)
	{
		return get(mTypes.get(aKey).getIndex());
	}


	public Object get(int aIndex)
	{
		return mValues.get(aIndex);
	}


	public FieldType getType(String aKey)
	{
		return mTypes.get(aKey);
	}


	public FieldType[] getFields()
	{
		return mTypes.values().toArray(new FieldType[mTypes.size()]);
	}


	public void add(FieldType aFieldType, Object aValue)
	{
		mTypes.put(aFieldType.getName(), aFieldType);
		mValues.put(aFieldType.getIndex(), aValue);
	}
}

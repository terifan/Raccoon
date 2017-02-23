package org.terifan.raccoon.util;

import java.util.HashMap;
import java.util.Map;
import org.terifan.raccoon.serialization.FieldDescriptor;


public class ResultSet
{
	private final Map<String, FieldDescriptor> mTypes;
	private final Map<Integer, Object> mValues;


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


	public FieldDescriptor getType(String aKey)
	{
		return mTypes.get(aKey);
	}


	public FieldDescriptor[] getFields()
	{
		return mTypes.values().toArray(new FieldDescriptor[mTypes.size()]);
	}


	public void add(FieldDescriptor aFieldType, Object aValue)
	{
		mTypes.put(aFieldType.getName(), aFieldType);
		mValues.put(aFieldType.getIndex(), aValue);
	}


	@Override
	public String toString()
	{
		return "ResultSet{" + "mValues=" + mValues + ", mTypes=" + mTypes + '}';
	}
}

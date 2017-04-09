package org.terifan.raccoon;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.terifan.raccoon.hashtable.LeafEntry;
import org.terifan.raccoon.serialization.FieldDescriptor;
import org.terifan.raccoon.serialization.Marshaller;
import org.terifan.raccoon.util.ByteArrayBuffer;


public class ResultSet implements AutoCloseable
{
	private final Map<String, FieldDescriptor> mTypes;
	private final Map<Integer, Object> mValues;
	private final Iterator<LeafEntry> mIterator;
	private final TableType mTable;
	private final Marshaller mMarshaller;


	ResultSet()
	{
		mValues = new HashMap<>();
		mTypes = new HashMap<>();

		mIterator = null;
		mTable = null;
		mMarshaller = null;
	}


	ResultSet(TableType aTable, Iterator<LeafEntry> aIterator)
	{
		mValues = new HashMap<>();
		mTypes = new HashMap<>();

		mTable = aTable;
		mIterator = aIterator;
		mMarshaller = new Marshaller(mTable.getTable().getEntityDescriptor());

		mTable.getDatabase().getReadLock().lock();
	}


	public boolean containsKey(String aFieldName)
	{
		return mTypes.containsKey(aFieldName);
	}


	public Object get(String aFieldName)
	{
		return get(mTypes.get(aFieldName).getIndex());
	}


	public Object get(int aFieldIndex)
	{
		return mValues.get(aFieldIndex);
	}


	public FieldDescriptor getField(String aFieldName)
	{
		return mTypes.get(aFieldName);
	}


	public FieldDescriptor[] getFields()
	{
		return mTypes.values().toArray(new FieldDescriptor[mTypes.size()]);
	}


	// TODO: hide
	public void add(FieldDescriptor aField, Object aValue)
	{
		mTypes.put(aField.getName(), aField);
		mValues.put(aField.getIndex(), aValue);
	}


	@Override
	public String toString()
	{
		return "ResultSet{" + "mValues=" + mValues + ", mTypes=" + mTypes + '}';
	}


	@Override
	public void close()
	{
		mTable.getDatabase().getReadLock().unlock();
	}


	public boolean next()
	{
		if (!mIterator.hasNext())
		{
			return false;
		}

		LeafEntry entry = mIterator.next();

		mMarshaller.unmarshal(new ByteArrayBuffer(entry.getKey()), this, Table.FIELD_CATEGORY_KEY);
		mMarshaller.unmarshal(new ByteArrayBuffer(entry.getValue()), this, Table.FIELD_CATEGORY_DISCRIMINATOR + Table.FIELD_CATEGORY_VALUE);

		return true;
	}
}

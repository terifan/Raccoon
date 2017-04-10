package org.terifan.raccoon;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.terifan.raccoon.hashtable.LeafEntry;
import org.terifan.raccoon.serialization.FieldDescriptor;
import org.terifan.raccoon.serialization.Marshaller;
import org.terifan.raccoon.util.ByteArrayBuffer;


/**
 * ResultSet iterates items in a table.
 *
 * Note: ResultSet instances open a read lock on the database and must always be closed. Concurrent write operations may cause dead locks!
 */
public class ResultSet implements AutoCloseable
{
	private final TableType mTable;
	private final Iterator<LeafEntry> mIterator;
	private final Marshaller mMarshaller;
	private final FieldDescriptor[] mTypes;
	private final Map<FieldDescriptor, Object> mValues;
	private final HashMap<String,FieldDescriptor> mTypeNames;


	ResultSet(FieldDescriptor[] aTypes)
	{
		mValues = new HashMap<>();
		mTypes = aTypes;
		mIterator = null;
		mTable = null;
		mMarshaller = null;

		mTypeNames = new HashMap<>();
		Arrays.stream(mTypes).forEach(e->mTypeNames.put(e.getName(), e));
	}


	ResultSet(TableType aTable, Iterator<LeafEntry> aIterator)
	{
		mValues = new HashMap<>();

		mTable = aTable;
		mTypes = mTable.getTable().getFields();
		mIterator = aIterator;
		mMarshaller = new Marshaller(mTable.getTable().getEntityDescriptor());

		mTypeNames = new HashMap<>();
		Arrays.stream(mTypes).forEach(e->mTypeNames.put(e.getName(), e));

		mTable.getDatabase().getReadLock().lock();
	}


	public Object get(String aFieldName)
	{
		return get(mTypeNames.get(aFieldName));
	}


	public Object get(int aFieldIndex)
	{
		return mValues.get(mTypes[aFieldIndex]);
	}


	public Object get(FieldDescriptor aFieldType)
	{
		return mValues.get(aFieldType);
	}


	public FieldDescriptor getField(String aFieldName)
	{
		return mTypeNames.get(aFieldName);
	}


	public FieldDescriptor getField(int aFieldIndex)
	{
		return mTypes[aFieldIndex];
	}


	public FieldDescriptor[] getFields()
	{
		return mTypes.clone();
	}


	// TODO: protect
	public void set(FieldDescriptor aField, Object aValue)
	{
		mValues.put(aField, aValue);
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

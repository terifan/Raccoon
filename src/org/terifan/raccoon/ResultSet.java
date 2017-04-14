package org.terifan.raccoon;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import org.terifan.raccoon.hashtable.LeafEntry;
import org.terifan.raccoon.serialization.EntityDescriptor;
import org.terifan.raccoon.serialization.FieldDescriptor;
import org.terifan.raccoon.serialization.Marshaller;
import org.terifan.raccoon.util.ByteArrayBuffer;


/**
 * ResultSet iterates items in a table.
 *
 * Note: ResultSet instances open a read lock on the database and must always be closed. Concurrent write operations may cause dead locks!
 */
public class ResultSet
{
	private final TableType mTable;
	private final Iterator<LeafEntry> mIterator;
	private final Marshaller mMarshaller;
	private final FieldDescriptor[] mTypes;
	private final Object[] mValues;
	private final HashMap<String,FieldDescriptor> mTypeNameLookup;


	ResultSet(EntityDescriptor aEntityDescriptor)
	{
		mTypes = aEntityDescriptor.getFields();
		mMarshaller = new Marshaller(aEntityDescriptor);
		mIterator = null;
		mTable = null;

		mValues = new Object[mTypes.length];
		mTypeNameLookup = new HashMap<>();
		Arrays.stream(mTypes).forEach(e->mTypeNameLookup.put(e.getName(), e));
	}


	ResultSet(TableType aTable, Iterator<LeafEntry> aIterator)
	{
		mTable = aTable;
		mIterator = aIterator;
		mMarshaller = new Marshaller(mTable.getTable().getEntityDescriptor());

		mTypes = mTable.getTable().getFields();
		mValues = new Object[mTypes.length];
		mTypeNameLookup = new HashMap<>();
		Arrays.stream(mTypes).forEach(e->mTypeNameLookup.put(e.getName(), e));
	}


	ResultSet unmarshal(ByteArrayBuffer aFieldData, int aFieldCategories)
	{
		mMarshaller.unmarshal(aFieldData, this, aFieldCategories);

		return this;
	}


	public Object get(int aFieldIndex)
	{
		return mValues[aFieldIndex];
	}


	public Object get(String aFieldName)
	{
		return get(mTypeNameLookup.get(aFieldName));
	}


	public Object get(FieldDescriptor aFieldType)
	{
		return mValues[aFieldType.getIndex()];
	}


	public FieldDescriptor getField(int aFieldIndex)
	{
		return mTypes[aFieldIndex];
	}


	public FieldDescriptor getField(String aFieldName)
	{
		return mTypeNameLookup.get(aFieldName);
	}


	public FieldDescriptor[] getFields()
	{
		return mTypes;
	}


	public void set(int aIndex, Object aValue)
	{
		mValues[aIndex] = aValue;
	}


	boolean next()
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


	@Override
	public String toString()
	{
		return "ResultSet{" + "mTypes=" + Arrays.toString(mTypes) + ", mValues=" + mValues + '}';
	}
}
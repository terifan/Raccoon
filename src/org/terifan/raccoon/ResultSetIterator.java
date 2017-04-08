package org.terifan.raccoon;

import org.terifan.raccoon.hashtable.LeafEntry;
import java.util.Iterator;
import org.terifan.raccoon.serialization.Marshaller;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.ResultSet;


public final class ResultSetIterator implements Iterator<ResultSet>
{
	private final Iterator<LeafEntry> mIterator;
	private final Table mTable;


	ResultSetIterator(Table aTable, Iterator<LeafEntry> aIterator)
	{
		mIterator = aIterator;
		mTable = aTable;
	}


	@Override
	public boolean hasNext()
	{
		return mIterator.hasNext();
	}


	@Override
	public ResultSet next()
	{
		LeafEntry entry = mIterator.next();
	
		ResultSet resultSet = new ResultSet();

		Marshaller marshaller = new Marshaller(mTable.getTableMetadata().getEntityDescriptor());

		marshaller.unmarshal(new ByteArrayBuffer(entry.getKey()), resultSet, TableMetadata.FIELD_CATEGORY_KEY);
		marshaller.unmarshal(new ByteArrayBuffer(entry.getValue()), resultSet, TableMetadata.FIELD_CATEGORY_DISCRIMINATOR + TableMetadata.FIELD_CATEGORY_VALUE);

		return resultSet;
	}


	@Override
	public void remove()
	{
	}
}

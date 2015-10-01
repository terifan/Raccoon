package org.terifan.raccoon;

import java.util.Iterator;
import org.terifan.raccoon.serialization.FieldCategory;


public class EntityIterator<T> implements Iterator<T>
{
	private final Iterator<Entry> mIterator;
	private final Table mTable;


	EntityIterator(Table aTable, Iterator<Entry> aIterator)
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
	public T next()
	{
		T outputEntity = (T)mTable.newEntityInstance();

		Entry entry = mIterator.next();

		mTable.update(outputEntity, entry.getKey(), FieldCategory.KEY);
		mTable.update(outputEntity, entry.getValue(), FieldCategory.DISCRIMINATOR_VALUE);

		return outputEntity;
	}


	@Override
	public void remove()
	{
		mIterator.remove();
	}
}
package org.terifan.raccoon;

import java.util.Iterator;
import org.terifan.raccoon.serialization.FieldCategoryFilter;


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

		mTable.unmarshalToObject(outputEntity, entry.getKey(), FieldCategoryFilter.KEYS);
		mTable.unmarshalToObject(outputEntity, entry.getValue(), FieldCategoryFilter.DISCRIMINATORS_VALUES);

		Initializer initializer = mTable.getDatabase().getInitializer(mTable.getTableMetadata().getType());
		if (initializer != null)
		{
			initializer.initialize(outputEntity);
		}

		return outputEntity;
	}


	@Override
	public void remove()
	{
		mIterator.remove();
	}
}
package org.terifan.raccoon;

import java.util.Iterator;


public class EntityIterator<T> implements Iterator<T>
{
	private Iterator<Entry> mIterator;
	private Table mTable;


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

		Initializer initializer = mTable.getInitializer();
		if (initializer != null)
		{
			initializer.initialize(outputEntity);
		}

		Entry entry = mIterator.next();

		mTable.update(outputEntity, entry.getKey());
		mTable.update(outputEntity, entry.getValue());

		return outputEntity;
	}


	@Override
	public void remove()
	{
		mIterator.remove();
	}
}
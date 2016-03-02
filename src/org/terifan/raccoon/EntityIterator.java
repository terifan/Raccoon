package org.terifan.raccoon;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
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
		T outputEntity = (T)newEntityInstance();

		Entry entry = mIterator.next();

		mTable.unmarshalToObject(outputEntity, entry.getKey(), FieldCategoryFilter.KEYS);
		mTable.unmarshalToObject(outputEntity, entry.getValue(), FieldCategoryFilter.DISCRIMINATORS_VALUES);

		return outputEntity;
	}


	private Object newEntityInstance()
	{
		try
		{
			Class type = mTable.getTableMetadata().getType();

			Factory factory = mTable.getDatabase().getFactory(type);

			if (factory != null)
			{
				return factory.newInstance();
			}
			else
			{
				Constructor constructor = type.getDeclaredConstructor();
				constructor.setAccessible(true);

				return constructor.newInstance();
			}
		}
		catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e)
		{
			throw new DatabaseException(e);
		}
	}


	@Override
	public void remove()
	{
		mIterator.remove();
	}
}
package org.terifan.raccoon;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;


public class EntityIterator<T> implements Iterator<T>
{
	private final Iterator<LeafEntry> mIterator;
	private final Table mTable;


	EntityIterator(Table aTable, Iterator<LeafEntry> aIterator)
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

		LeafEntry entry = mIterator.next();

		mTable.unmarshalToObjectKeys(outputEntity, entry);
		mTable.unmarshalToObjectValues(outputEntity, entry);

		initializeNewEntity(outputEntity);

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

	private void initializeNewEntity(T aEntity)
	{
		Class type = mTable.getTableMetadata().getType();

		Initializer initializer = mTable.getDatabase().getInitializer(type);

		if (initializer != null)
		{
			initializer.initialize(aEntity);
		}
	}


	@Override
	public void remove()
	{
		mIterator.remove();
	}

}
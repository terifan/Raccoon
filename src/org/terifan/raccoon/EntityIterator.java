package org.terifan.raccoon;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.function.Supplier;


public final class EntityIterator<T> implements Iterator<T>
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

		mTable.unmarshalToObjectKeys(entry, outputEntity);
		mTable.unmarshalToObjectValues(entry, outputEntity);

		initializeNewEntity(outputEntity);

		return outputEntity;
	}


	private Object newEntityInstance()
	{
		try
		{
			Class type = mTable.getTableMetadata().getType();

			Supplier supplier = mTable.getDatabase().getSupplier(type);

			if (supplier != null)
			{
				return supplier.get();
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

		if (type == TableMetadata.class)
		{
			((TableMetadata)aEntity).initialize();
		}
		else
		{
			Initializer initializer = mTable.getDatabase().getInitializer(type);

			if (initializer != null)
			{
				initializer.initialize(aEntity);
			}
			if (aEntity instanceof Initializable)
			{
				((Initializable)aEntity).initialize();
			}
		}
	}


	@Override
	public void remove()
	{
		mIterator.remove();
	}

}

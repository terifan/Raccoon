	package org.terifan.raccoon;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.function.Supplier;
import org.terifan.raccoon.util.Log;


final class EntityIterator<T> implements Iterator<T>
{
	private final Iterator<ArrayMapEntry> mIterator;
	private final TableInstance mTableInstance;


	EntityIterator(TableInstance aTable, Iterator<ArrayMapEntry> aIterator)
	{
		mIterator = aIterator;
		mTableInstance = aTable;
	}


	@Override
	public boolean hasNext()
	{
		return mIterator.hasNext();
	}


	@Override
	public T next()
	{
		Log.d("next entity");
		Log.inc();

		T outputEntity = (T)newEntityInstance();

		ArrayMapEntry entry = mIterator.next();

		mTableInstance.unmarshalToObjectKeys(entry, outputEntity);
		mTableInstance.unmarshalToObjectValues(entry, outputEntity);

		initializeNewEntity(outputEntity);

		Log.dec();

		return outputEntity;
	}


	private Object newEntityInstance()
	{
		Log.d("new entity instance");
		Log.inc();

		try
		{
			Class type = mTableInstance.getTable().getType();

			Supplier supplier = mTableInstance.getDatabase().getSupplier(type);

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
		finally
		{
			Log.dec();
		}
	}


	private void initializeNewEntity(T aEntity)
	{
		Class type = mTableInstance.getTable().getType();

		if (type == Table.class)
		{
			((Table)aEntity).initialize(mTableInstance.getDatabase());
		}
		else
		{
			Initializer initializer = mTableInstance.getDatabase().getInitializer(type);

			if (initializer != null)
			{
				initializer.initialize(aEntity);
			}
			else if (aEntity instanceof Initializable)
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

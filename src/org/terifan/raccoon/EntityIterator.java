	package org.terifan.raccoon;

import org.terifan.raccoon.core.RecordEntry;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.function.Supplier;
import org.terifan.raccoon.util.Log;


final class EntityIterator<T> implements Iterator<T>
{
	private final Iterator<RecordEntry> mIterator;
	private final TableInstance mTable;


	EntityIterator(TableInstance aTable, Iterator<RecordEntry> aIterator)
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
		Log.d("next entity");
		Log.inc();

		T outputEntity = (T)newEntityInstance();

		RecordEntry entry = mIterator.next();

		mTable.unmarshalToObjectKeys(entry, outputEntity);
		mTable.unmarshalToObjectValues(entry, outputEntity);

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
			Class type = mTable.getTable().getType();

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
		finally
		{
			Log.dec();
		}
	}


	private void initializeNewEntity(T aEntity)
	{
		Class type = mTable.getTable().getType();

		if (type == Table.class)
		{
			((Table)aEntity).initialize(mTable.getDatabase());
		}
		else
		{
			Initializer initializer = mTable.getDatabase().getInitializer(type);

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

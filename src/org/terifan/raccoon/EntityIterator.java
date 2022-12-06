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
	private final Database mDatabase;


	EntityIterator(Database aDatabase, TableInstance aTable, Iterator<ArrayMapEntry> aIterator)
	{
		mDatabase = aDatabase;
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
		TransactionGroup tx = mDatabase.getTransactionGroup();

		mTableInstance.unmarshalToObjectKeys(entry, outputEntity);
		mTableInstance.unmarshalToObjectValues(mDatabase, entry, outputEntity, tx);

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

			Supplier supplier = mDatabase.getSupplier(type);

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
			((Table)aEntity).initialize(mDatabase);
		}
		else
		{
			Initializer initializer = mDatabase.getInitializer(type);

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

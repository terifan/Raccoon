	package org.terifan.raccoon;

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

		RecordEntry entry = mIterator.next();

		T outputEntity = (T)newEntityInstance(entry);

		mTable.unmarshalToObjectKeys(entry, outputEntity);
		mTable.unmarshalToObjectValues(entry, outputEntity);

		initializeNewEntity(outputEntity);

		mTable.getCost().mUnmarshalEntity++;

		Log.dec();

		return outputEntity;
	}


	private Object newEntityInstance(RecordEntry aEntry)
	{
		Log.d("new entity instance");
		Log.inc();

		try
		{
			Class type = mTable.getTable().getType();

//			ClassifiedSupplier classifiedSupplier = mTable.getDatabase().getClassifier(type);
//
//			if (classifiedSupplier != null)
//			{
//				System.out.println("#");
//
//				ResultSet discriminators = mTable.unmarshalDiscriminators(aEntry);
//
//				System.out.println(discriminators);
//			}

			Supplier supplier = mTable.getDatabase().getSupplier(type);

			if (supplier != null)
			{
				return supplier.get();
			}

			Constructor constructor = type.getDeclaredConstructor();
			constructor.setAccessible(true);

			return constructor.newInstance();
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

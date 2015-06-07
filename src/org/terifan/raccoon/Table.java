package org.terifan.raccoon;

import org.terifan.raccoon.io.BlockPointer;
import org.terifan.raccoon.serialization.FieldCategory;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.terifan.raccoon.hashtable.HashTable;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;


class Table<T> implements Iterable<T>
{
	@Key private String mName;
	@Key private byte[] mDiscriminator;
	private byte[] mPointer;
	private String mType;
	private long mHashSeed;

	private transient Database mDatabase;
	private transient TableType mTableType;
	private transient HashTable mTableImplementation;


	Table(Database aDatabase, TableType aTableType, Object aDiscriminator) throws IOException
	{
		mDatabase = aDatabase;
		mTableType = aTableType;

		mName = mTableType.getName();
		mType = mTableType.getType().getName();

		if (aDiscriminator != null)
		{
			Log.v("find discriminator");
			Log.inc();

			mDiscriminator = mTableType.getMarshaller().marshal(new ByteArrayBuffer(16), aDiscriminator, FieldCategory.DISCRIMINATOR).trim().array();

			if (mDiscriminator.length == 0)
			{
				mDiscriminator = null;
			}

			Log.dec();
		}
	}


	Table open(BlockPointer aBlockPointer) throws IOException
	{
		Log.i("open table");
		Log.inc();

		if (aBlockPointer == null && mPointer != null)
		{
			aBlockPointer = new BlockPointer().unmarshal(mPointer, 0);
		}

		if (mPointer == null)
		{
			if (mTableType.getType() == Table.class)
			{
				mHashSeed = 0x654196f4434970d4L;
			}
			else
			{
				mHashSeed = new SecureRandom().nextLong();
			}

//			ByteArrayOutputStream baos = new ByteArrayOutputStream();
//			try (ObjectOutputStream oos = new ObjectOutputStream(baos))
//			{
//				oos.writeObject(mTableType.getMarshaller().getTypeDeclarations());
//			}
//			catch (IOException e)
//			{
//				throw new DatabaseException(e);
//			}
//			return baos.toByteArray();
		}

		mTableImplementation = new HashTable(mDatabase.getBlockDevice(), aBlockPointer, mHashSeed, 4*mDatabase.getBlockDevice().getBlockSize(), 8*mDatabase.getBlockDevice().getBlockSize(), mDatabase.getTransactionId());

		Log.dec();

		return this;
	}


	public boolean save(T aEntity)
	{
		Log.i("save entity");
		Log.inc();

		byte[] key = getKeys(aEntity);
		byte[] value = getValues(aEntity);
		boolean b = mTableImplementation.put(key, value, mDatabase.getTransactionId());

		Log.dec();

		return b;
	}


	public boolean get(T aEntity)
	{
		Log.i("get entity");
		Log.inc();

		byte[] key = getKeys(aEntity);
		byte[] value = mTableImplementation.get(key);

		if (value == null)
		{
			Log.dec();

			return false;
		}

		update(aEntity, value, FieldCategory.VALUE);

		Log.dec();

		return true;
	}


	public boolean contains(T aEntity)
	{
		byte[] key = getKeys(aEntity);
		return mTableImplementation.contains(key);
	}


	public <T> List<T> list(Class<T> aType)
	{
		ArrayList<T> list = new ArrayList<>();
		iterator().forEachRemaining(e -> list.add((T)e));
		return list;
	}


	public Database getDatabase()
	{
		return mDatabase;
	}


	public synchronized InputStream read(T aEntity)
	{
		return mTableImplementation.read(getKeys(aEntity));
	}


	public boolean save(T aEntity, InputStream aInputStream)
	{
		return mTableImplementation.put(getKeys(aEntity), aInputStream, mDatabase.getTransactionId());
	}


	public boolean remove(T aEntity)
	{
		return mTableImplementation.remove(getKeys(aEntity), mDatabase.getTransactionId());
	}


	@Override
	public Iterator<T> iterator()
	{
		return new EntityIterator(this, mTableImplementation.iterator());
	}


	public Iterator<Entry> iteratorRaw()
	{
		return mTableImplementation.iterator();
	}


	public void clear() throws IOException
	{
		mTableImplementation.clear(mDatabase.getTransactionId());
	}


	BlockPointer getRootBlockPointer()
	{
		return mTableImplementation.getRootBlockPointer();
	}


	void close() throws IOException
	{
		mTableImplementation.close();
	}


	boolean commit() throws IOException
	{
		if (!mTableImplementation.commit(mDatabase.getTransactionId()))
		{
			return false;
		}

		byte[] pointer = mTableImplementation.getRootBlockPointer().marshal(new byte[BlockPointer.SIZE], 0);

		boolean wasUpdated = !Arrays.equals(pointer, mPointer);

		mPointer = pointer;

		return wasUpdated;
	}


	void rollback() throws IOException
	{
		mTableImplementation.rollback();
	}


	int size() throws IOException
	{
		return mTableImplementation.size();
	}


	String integrityCheck()
	{
		return mTableImplementation.integrityCheck();
	}


	public Initializer getInitializer()
	{
		return mDatabase.getInitializer(mTableType.getType());
	}


	byte[] getDiscriminators(Object aInput)
	{
		return mTableType.getMarshaller().marshal(new ByteArrayBuffer(16), aInput, FieldCategory.DISCRIMINATOR).trim().array();
	}


	byte[] getKeys(Object aInput)
	{
		return mTableType.getMarshaller().marshal(new ByteArrayBuffer(16), aInput, FieldCategory.KEY).trim().array();
	}


	byte[] getValues(Object aInput)
	{
		return mTableType.getMarshaller().marshal(new ByteArrayBuffer(16), aInput, FieldCategory.VALUE).trim().array();
	}


	void update(Object aOutput, byte[] aMarshalledData, FieldCategory aCategory)
	{
		mTableType.getMarshaller().unmarshal(new ByteArrayBuffer(aMarshalledData), aOutput, aCategory);
	}


	Object newEntityInstance()
	{
		try
		{
			Constructor constructor = mTableType.getType().getDeclaredConstructor();
			constructor.setAccessible(true);

			Object object = constructor.newInstance();

			if (mDiscriminator != null)
			{
				mTableType.getMarshaller().unmarshal(new ByteArrayBuffer(mDiscriminator), object, FieldCategory.DISCRIMINATOR);
			}

			return object;
		}
		catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e)
		{
			throw new DatabaseException(e);
		}
	}


	@Override
	public String toString()
	{
		return mName;
	}
}

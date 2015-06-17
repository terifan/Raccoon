package org.terifan.raccoon;

import java.io.ByteArrayInputStream;
import org.terifan.raccoon.serialization.FieldCategory;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.terifan.raccoon.hashtable.HashTable;
import org.terifan.raccoon.io.Blob;
import org.terifan.raccoon.io.BlobInputStream;
import org.terifan.raccoon.io.BlobOutputStream;
import org.terifan.raccoon.io.BlockAccessor;
import org.terifan.raccoon.io.IManagedBlockDevice;
import org.terifan.raccoon.io.Streams;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;


class Table<T> implements Iterable<T>
{
	@Key private String mName;
	@Key private byte[] mDiscriminator;
	private byte[] mPointer;
	private String mType;

	private transient Database mDatabase;
	private transient TableType mTableType;
	private transient HashTable mTableImplementation;
	private transient BlockAccessor mBlockAccessor;
	private transient IManagedBlockDevice mBlockDevice;
	private transient Initializer mInitializer;


	Table(Database aDatabase, TableType aTableType, Object aDiscriminator) throws IOException
	{
		mDatabase = aDatabase;
		mTableType = aTableType;
		mBlockDevice = mDatabase.getBlockDevice();
		mBlockAccessor = new BlockAccessor(mBlockDevice);
		mInitializer = mDatabase.getInitializer(mTableType.getType());

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


	Table open(ByteArrayBuffer aTableHeader) throws IOException
	{
		Log.i("open table");
		Log.inc();

		if (aTableHeader == null && mPointer != null)
		{
			aTableHeader = new ByteArrayBuffer(mPointer);
		}

//		if (mPointer == null)
//		{
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
//		}

		mTableImplementation = new HashTable(mBlockDevice, aTableHeader, getTransactionId(), false);

		Log.dec();

		return this;
	}


	private long getTransactionId()
	{
		return mDatabase.getTransactionId();
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
		return mTableImplementation.containsKey(key);
	}


	public <T> List<T> list(Class<T> aType)
	{
		ArrayList<T> list = new ArrayList<>();
		iterator().forEachRemaining(e -> list.add((T)e));
		return list;
	}


	public synchronized InputStream read(T aEntity)
	{
		byte[] key = getKeys(aEntity);

		byte[] value = mTableImplementation.get(key);

		if (value == null)
		{
			return null;
		}

		if (value[0] == 0)
		{
			return new ByteArrayInputStream(value, 1, value.length - 1);
		}

		try
		{
			return new BlobInputStream(mBlockAccessor, new ByteArrayBuffer(value).position(1));
		}
		catch (Exception e)
		{
			throw new DatabaseException(e);
		}
	}


	public boolean save(T aEntity)
	{
		Log.i("save entity");
		Log.inc();

		byte[] key = getKeys(aEntity);
		byte[] tmp = getValues(aEntity);
		byte[] value;
		byte type = 0;

		if ((key.length + tmp.length + 1) > mTableImplementation.getEntryMaximumLength() / 4)
		{
			type = 1;

			try (BlobOutputStream bos = new BlobOutputStream(mBlockAccessor, getTransactionId()))
			{
				bos.write(tmp);
				tmp = bos.finish();
			}
			catch (IOException e)
			{
				throw new DatabaseException(e);
			}
		}

		value = new byte[1 + tmp.length];
		value[0] = type;
		System.arraycopy(tmp, 0, value, 1, tmp.length);

		byte[] oldValue = mTableImplementation.put(key, value, getTransactionId());

		deleteIfBlob(oldValue);

		Log.dec();

		return oldValue == null;
	}


	private void deleteIfBlob(byte[] aOldValue) throws DatabaseException
	{
		if (aOldValue != null && aOldValue[0] == 1)
		{
			try
			{
				Blob.deleteBlob(mBlockAccessor, Arrays.copyOfRange(aOldValue, 1, aOldValue.length - 1));
			}catch (IOException e)
			{
				throw new DatabaseException(e);
			}
		}
	}


	public boolean save(T aEntity, InputStream aInputStream)
	{
		try (BlobOutputStream bos = new BlobOutputStream(mBlockAccessor, getTransactionId()))
		{
			bos.write(Streams.fetch(aInputStream));

			byte[] tmp = bos.finish();
			byte[] value = new byte[1 + tmp.length];
			value[0] = 1;
			System.arraycopy(tmp, 0, value, 1, tmp.length);

			byte[] oldValue = mTableImplementation.put(getKeys(aEntity), value, getTransactionId());

			deleteIfBlob(oldValue);

			return oldValue == null;
		}
		catch (IOException e)
		{
			throw new DatabaseException(e);
		}
	}


	public boolean remove(T aEntity)
	{
		byte[] oldValue = mTableImplementation.remove(getKeys(aEntity), getTransactionId());

		deleteIfBlob(oldValue);

		return oldValue != null;
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
		mTableImplementation.clear(getTransactionId());
	}


	byte[] getTableHeader()
	{
		return mTableImplementation.getTableHeader();
	}


	void close() throws IOException
	{
		mTableImplementation.close();
	}


	boolean commit() throws IOException
	{
		if (!mTableImplementation.commit(getTransactionId()))
		{
			return false;
		}

		byte[] newPointer = mTableImplementation.getTableHeader();

		boolean wasUpdated = !Arrays.equals(newPointer, mPointer);

		mPointer = newPointer;

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
		ByteArrayBuffer buffer = new ByteArrayBuffer(aMarshalledData);

		if (aCategory == FieldCategory.VALUE)
		{
			if (buffer.read() == 1)
			{
				try
				{
					buffer.wrap(Streams.fetch(new BlobInputStream(mBlockAccessor, buffer)));
					buffer.position(0);
				}
				catch (Exception e)
				{
					Log.dec();

					throw new DatabaseException(e);
				}
			}
		}

		mTableType.getMarshaller().unmarshal(buffer, aOutput, aCategory);
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

			if (mInitializer != null)
			{
				mInitializer.initialize(object);
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

package org.terifan.raccoon;

import org.terifan.raccoon.serialization.old.FieldCategory;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import org.terifan.raccoon.io.Blob;
import org.terifan.raccoon.io.BlobInputStream;
import org.terifan.raccoon.io.BlobOutputStream;
import org.terifan.raccoon.io.BlockAccessor;
import org.terifan.raccoon.io.IManagedBlockDevice;
import org.terifan.raccoon.io.Streams;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;


public class Table<T> implements Iterable<T>
{
	static final byte PTR_RECORD = 0;
	static final byte PTR_BLOB = 1;

	private Database mDatabase;
	private TableMetadata mTableMetadata;
	private BlockAccessor mBlockAccessor;
	private IManagedBlockDevice mBlockDevice;
	private HashSet<BlobOutputStream> mOpenOutputStreams;
	private HashTable mTableImplementation;
	private byte[] mPointer;


	Table(Database aDatabase, TableMetadata aTableMetadata, byte[] aPointer)
	{
		try
		{
			mOpenOutputStreams = new HashSet<>();

			mDatabase = aDatabase;
			mTableMetadata = aTableMetadata;
			mPointer = aPointer;

			mBlockDevice = mDatabase.getBlockDevice();
			mTableImplementation = new HashTable(mBlockDevice, mPointer, mDatabase.getTransactionId(), false, mDatabase.getParameter(CompressionParam.class, null));

			mBlockAccessor = new BlockAccessor(mBlockDevice);

			CompressionParam parameter = mDatabase.getParameter(CompressionParam.class, null);
			if (parameter != null)
			{
				mBlockAccessor.setCompressionParam(parameter);
			}
		}
		catch (IOException e)
		{
			throw new DatabaseIOException(e);
		}
	}


	public Database getDatabase()
	{
		return mDatabase;
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

		unmarshalToObject(aEntity, value, FieldCategory.DISCRIMINATOR_AND_VALUES);

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

		ByteArrayBuffer buffer = new ByteArrayBuffer(value);

		if (buffer.read() == PTR_RECORD)
		{
			return buffer;
		}

		try
		{
			return new BlobInputStream(mBlockAccessor, buffer);
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
		byte[] value = getNonKeys(aEntity);
		byte type = PTR_RECORD;

		if ((key.length + value.length + 1) > mTableImplementation.getEntryMaximumLength() / 4)
		{
			type = PTR_BLOB;

			try (BlobOutputStream bos = new BlobOutputStream(mBlockAccessor, mDatabase.getTransactionId()))
			{
				bos.write(value);
				value = bos.finish();
			}
			catch (IOException e)
			{
				throw new DatabaseException(e);
			}
		}

		byte[] oldValue = putWrap(key, value, type);

		deleteIfBlob(oldValue);

		Log.dec();

		return oldValue == null;
	}


	private void deleteIfBlob(byte[] aOldValue) throws DatabaseException
	{
		if (aOldValue != null && aOldValue[0] == PTR_BLOB)
		{
			try
			{
				Blob.deleteBlob(mBlockAccessor, Arrays.copyOfRange(aOldValue, 1, aOldValue.length));
			}
			catch (IOException e)
			{
				throw new DatabaseException(e);
			}
		}
	}


	public BlobOutputStream saveBlob(T aEntityKey)
	{
		try
		{
			BlobOutputStream out = new BlobOutputStream(mBlockAccessor, mDatabase.getTransactionId());

			synchronized (this)
			{
				mOpenOutputStreams.add(out);
			}

			out.setOnCloseListener((aHeader)->
			{
				Log.v("write blob entry");

				byte[] oldValue = putWrap(getKeys(aEntityKey), aHeader, PTR_BLOB);

				deleteIfBlob(oldValue);

				synchronized (this)
				{
					mOpenOutputStreams.remove(out);
				}
			});

			return out;
		}
		catch (IOException e)
		{
			throw new DatabaseIOException(e);
		}
	}


	private byte[] putWrap(byte[] aKey, byte[] aValue, byte aType)
	{
		byte[] value = new byte[aValue.length + 1];
		System.arraycopy(aValue, 0, value, 1, aValue.length);

		value[0] = aType;

		return mTableImplementation.put(aKey, value);
	}


	public boolean save(T aEntity, InputStream aInputStream)
	{
		try (BlobOutputStream bos = new BlobOutputStream(mBlockAccessor, mDatabase.getTransactionId()))
		{
			bos.write(Streams.fetch(aInputStream));

			byte[] oldValue = putWrap(getKeys(aEntity), bos.finish(), PTR_BLOB);

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
		byte[] oldValue = mTableImplementation.remove(getKeys(aEntity));

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


	public void clear()
	{
		mTableImplementation.clear();
	}


	void close() throws IOException
	{
		mTableImplementation.close();
	}


	boolean isChanged()
	{
		return mTableImplementation.isChanged();
	}


	boolean commit()
	{
		synchronized (this)
		{
			if (!mOpenOutputStreams.isEmpty())
			{
				throw new DatabaseException("A table cannot be commited while a stream is open.");
			}
		}

		try
		{
			if (!mTableImplementation.commit())
			{
				return false;
			}
		}
		catch (IOException e)
		{
			throw new DatabaseIOException(e);
		}

		byte[] newPointer = mTableImplementation.getTableHeader();

		boolean wasUpdated = !Arrays.equals(newPointer, mPointer);

		mTableMetadata.setPointer(newPointer);

		return wasUpdated;
	}


	void rollback() throws IOException
	{
		mTableImplementation.rollback();
	}


	int size()
	{
		return mTableImplementation.size();
	}


	String integrityCheck()
	{
		return mTableImplementation.integrityCheck();
	}


	void unmarshalToObject(Object aOutput, byte[] aMarshalledData, FieldCategory aCategory)
	{
		ByteArrayBuffer buffer = new ByteArrayBuffer(aMarshalledData);

		if (aCategory == FieldCategory.VALUES || aCategory == FieldCategory.DISCRIMINATOR_AND_VALUES)
		{
			if (buffer.read() == PTR_BLOB)
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

		mTableMetadata.getMarshaller().unmarshal(buffer, aOutput, aCategory);

		Initializer<Object> initializer = (Initializer<Object>)mDatabase.getInitializer(aOutput.getClass());
		if (initializer != null)
		{
			initializer.initialize(aOutput);
		}
	}


	Object newEntityInstance()
	{
		try
		{
			Factory factory = mDatabase.getFactory(mTableMetadata.getType());
			Object object;

			if (factory != null)
			{
				object = factory.newInstance();
			}
			else
			{
				Constructor constructor = mTableMetadata.getType().getDeclaredConstructor();
				constructor.setAccessible(true);

				object = constructor.newInstance();
			}

			return object;
		}
		catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e)
		{
			throw new DatabaseException(e);
		}
	}


	byte[] getKeys(Object aInput)
	{
		return mTableMetadata.getMarshaller().marshal(new ByteArrayBuffer(16), aInput, FieldCategory.KEYS).trim().array();
	}


	byte[] getNonKeys(Object aInput)
	{
		return mTableMetadata.getMarshaller().marshal(new ByteArrayBuffer(16), aInput, FieldCategory.DISCRIMINATOR_AND_VALUES).trim().array();
	}


	public TableMetadata getTableMetadata()
	{
		return mTableMetadata;
	}


	@Override
	public String toString()
	{
		return mTableMetadata.toString();
	}


	public class TypeResult
	{
		public byte type;
	}
}

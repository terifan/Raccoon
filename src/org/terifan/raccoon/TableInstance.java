package org.terifan.raccoon;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.terifan.raccoon.storage.BlockAccessor;
import org.terifan.raccoon.serialization.Marshaller;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;


public final class TableInstance<T> implements Closeable
{
	public static final byte FLAG_BLOB = 1;

	private final Database mDatabase;
	private final Table mTable;
	private final HashSet<CommitLock> mCommitLocks;
	private final HashTable mHashTable;
	private final Cost mCost;


	TableInstance(Database aDatabase, Table aTable, byte[] aPointer)
	{
		try
		{
			mCost = new Cost();
			mCommitLocks = new HashSet<>();

			mDatabase = aDatabase;
			mTable = aTable;

			CompressionParam compression = mDatabase.getCompressionParameter();
			TableParam parameter = mDatabase.getTableParameter();

			mHashTable = new HashTable(mDatabase.getBlockDevice(), aPointer, mDatabase.getTransactionId(), false, compression, parameter, aTable.getTypeName(), mCost, aDatabase.getPerformanceTool());
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


	public Cost getCost()
	{
		return mCost;
	}


	public boolean get(T aEntity)
	{
		Log.i("get entity %s", aEntity);
		Log.inc();

		try
		{
			ArrayMapEntry entry = new ArrayMapEntry(getKeys(aEntity));

			if (mHashTable.get(entry))
			{
				unmarshalToObjectValues(entry, aEntity);

				Log.dec();

				return true;
			}
		}
		finally
		{
			Log.dec();
		}

		return false;
	}


	public <T> List<T> list(Class<T> aType, int aLimit)
	{
		ArrayList<T> list = new ArrayList<>();
		for (Iterator<T> it = (Iterator<T>)iterator(); list.size() < aLimit && it.hasNext();)
		{
			list.add(it.next());
			mCost.mEntityReturn++;
		}
		return list;
	}


//	public synchronized InputStream read(T aEntity)
//	{
//		ArrayMapEntry entry = new ArrayMapEntry(getKeys(aEntity));
//
//		if (!mHashTable.get(entry))
//		{
//			return null;
//		}
//
//		if (entry.hasFlag(FLAG_BLOB))
//		{
//			try
//			{
//				ByteArrayBuffer buffer = new ByteArrayBuffer(entry.getValue());
//
//				return new BlobInputStream(getBlockAccessor(), buffer);
//			}
//			catch (IOException e)
//			{
//				throw new DatabaseException(e);
//			}
//		}
//
//		return new ByteArrayInputStream(entry.getValue());
//	}


	public boolean save(T aEntity)
	{
		Log.i("save %s", aEntity.getClass());
		Log.inc();

		byte[] key = getKeys(aEntity);
		byte[] value = getNonKeys(aEntity);
		byte type = 0;

		if (key.length + value.length + 1 > mHashTable.getEntryMaximumLength() / 4)
		{
			type = FLAG_BLOB;

			try
			{
				Blob blob = new Blob(getBlockAccessor(), mDatabase.getTransactionId(), new ByteArrayBuffer(0), BlobOpenOption.WRITE);
				blob.write(ByteBuffer.wrap(value));
				blob.close();
				value = blob.getHeader();
			}
			catch (IOException e)
			{
				throw new DatabaseException(e);
			}
		}

		ArrayMapEntry entry = new ArrayMapEntry(key, value, type);

		if (mHashTable.put(entry))
		{
			deleteIfBlob(entry);
		}

		Log.dec();

		return entry.getValue() != null;
	}


	private void deleteIfBlob(ArrayMapEntry aEntry) throws DatabaseException
	{
		if (aEntry.hasFlag(FLAG_BLOB))
		{
			try
			{
				Blob_.deleteBlob(getBlockAccessor(), aEntry.getValue());
			}
			catch (IOException e)
			{
				throw new DatabaseException(e);
			}
		}
	}


	public Blob openBlob(T aEntityKey, BlobOpenOption aOpenOption)
	{
		try
		{
			CommitLock lock = new CommitLock(new Exception());

			byte[] key = getKeys(aEntityKey);
			ArrayMapEntry entry = new ArrayMapEntry(key);

			ByteArrayBuffer header;

			if (mHashTable.get(entry))
			{
				if (!entry.hasFlag(FLAG_BLOB))
				{
					throw new IllegalArgumentException("Not a blob");
				}

				if (aOpenOption == BlobOpenOption.CREATE)
				{
					throw new IllegalArgumentException("A blob already exists with this key.");
				}

				if (aOpenOption == BlobOpenOption.REPLACE)
				{
					deleteIfBlob(entry);
				}

				header = new ByteArrayBuffer(entry.getValue());
			}
			else
			{
				header = new ByteArrayBuffer(0);
			}

			Blob out = new Blob(getBlockAccessor(), mDatabase.getTransactionId(), header, aOpenOption)
			{
				@Override
				void onException(Exception aException)
				{
				}

				@Override
				void onClose()
				{
					Log.d("write blob entry");

					mDatabase.aquireWriteLock();
					try
					{
						ArrayMapEntry entry = new ArrayMapEntry(key, getHeader(), FLAG_BLOB);

						if (mHashTable.put(entry))
						{
							deleteIfBlob(entry);
						}
					}
					catch (DatabaseException e)
					{
						mDatabase.forceClose(e);
						throw e;
					}
					finally
					{
						mDatabase.releaseWriteLock();

						synchronized (TableInstance.this)
						{
							mCommitLocks.remove(lock);
						}
					}
				}
			};

			lock.setBlob(out);

			synchronized (this)
			{
				mCommitLocks.add(lock);
			}

			return out;
		}
		catch (IOException e)
		{
			throw new DatabaseIOException(e);
		}
	}


//	public boolean save(T aEntity, InputStream aInputStream)
//	{
//		try (BlobOutputStream bos = new BlobOutputStream(getBlockAccessor(), mDatabase.getTransactionId(), null))
//		{
//			Streams.transfer(aInputStream, bos);
//
//			byte[] key = getKeys(aEntity);
//
//			ArrayMapEntry entry = new ArrayMapEntry(key, bos.finish(), FLAG_BLOB);
//
//			if (mHashTable.put(entry))
//			{
//				deleteIfBlob(entry);
//			}
//
//			return entry.getValue() == null;
//		}
//		catch (IOException e)
//		{
//			throw new DatabaseException(e);
//		}
//	}


	public boolean remove(T aEntity)
	{
		ArrayMapEntry entry = new ArrayMapEntry(getKeys(aEntity));

		if (mHashTable.remove(entry))
		{
			deleteIfBlob(entry);

			return true;
		}

		return false;
	}


	/**
	 * Creates an iterator over all items in this table. This iterator will reconstruct entities.
	 */
	public Iterator<T> iterator()
	{
		return new EntityIterator(this, getLeafIterator());
	}


	Iterator<ArrayMapEntry> getLeafIterator()
	{
		return mHashTable.iterator();
	}


	public void clear()
	{
		mHashTable.clear();
	}


	@Override
	public void close()
	{
		mHashTable.close();
	}


	boolean isModified()
	{
		return mHashTable.isChanged();
	}


	boolean commit()
	{
		synchronized (this)
		{
			if (!mCommitLocks.isEmpty())
			{
				throw new CommitBlockedException("A table cannot be commited while a stream is open.");
			}
		}

		try
		{
			if (!mHashTable.commit())
			{
				return false;
			}
		}
		catch (IOException e)
		{
			throw new DatabaseIOException(e);
		}

		byte[] newPointer = mHashTable.marshalHeader();

		if (Arrays.equals(newPointer, mTable.getTableHeader()))
		{
			return false;
		}

		mTable.setTableHeader(newPointer);

		return true;
	}


	void rollback() throws IOException
	{
		mHashTable.rollback();
	}


	int size()
	{
		return mHashTable.size();
	}


	String integrityCheck()
	{
		return mHashTable.integrityCheck();
	}


	void unmarshalToObjectKeys(ArrayMapEntry aBuffer, Object aOutput)
	{
		ByteArrayBuffer buffer = new ByteArrayBuffer(aBuffer.getKey());

		mTable.getMarshaller().unmarshal(buffer, aOutput, Table.FIELD_CATEGORY_KEY);

		mCost.mUnmarshalKeys++;
	}


	void unmarshalToObjectValues(ArrayMapEntry aBuffer, Object aOutput)
	{
		ByteArrayBuffer buffer = new ByteArrayBuffer(aBuffer.getValue());

		if (aBuffer.hasFlag(FLAG_BLOB))
		{
			try (Blob blob = new Blob(getBlockAccessor(), mDatabase.getTransactionId(), new ByteArrayBuffer(aBuffer.getValue()), BlobOpenOption.READ))
			{
				byte[] buf = new byte[(int)blob.size()];
				blob.read(ByteBuffer.wrap(buf));
				buffer.write(buf);
				buffer.position(0);
			}
			catch (IOException e)
			{
				throw new DatabaseException(e);
			}
		}

		Marshaller marshaller = mTable.getMarshaller();
		marshaller.unmarshal(buffer, aOutput, Table.FIELD_CATEGORY_DISCRIMINATOR | Table.FIELD_CATEGORY_VALUE);

		mCost.mUnmarshalValues++;
	}


	private byte[] getKeys(Object aInput)
	{
		return mTable.getMarshaller().marshal(new ByteArrayBuffer(16), aInput, Table.FIELD_CATEGORY_KEY).trim().array();
	}


	private byte[] getNonKeys(Object aInput)
	{
		ByteArrayBuffer buffer = new ByteArrayBuffer(16);

		Marshaller marshaller = mTable.getMarshaller();
		marshaller.marshal(buffer, aInput, Table.FIELD_CATEGORY_DISCRIMINATOR | Table.FIELD_CATEGORY_VALUE);

		return buffer.trim().array();
	}


	public Table getTable()
	{
		return mTable;
	}


	@Override
	public String toString()
	{
		return mTable.toString();
	}


	void scan(ScanResult aScanResult)
	{
		mHashTable.scan(aScanResult);
	}


	<T> Stream<T> stream(Lock aReadLock)
	{
		try
		{
			Stream<T> tmp = StreamSupport.stream(new Spliterators.AbstractSpliterator<T>(Long.MAX_VALUE, Spliterator.IMMUTABLE | Spliterator.NONNULL)
			{
				EntityIterator entityIterator = new EntityIterator(TableInstance.this, mHashTable.iterator());

				@Override
				public boolean tryAdvance(Consumer<? super T> aConsumer)
				{
					if (!entityIterator.hasNext())
					{
						aReadLock.unlock();

						return false;
					}
					aConsumer.accept((T)entityIterator.next());
					return true;
				}
			}, false);

			return tmp;
		}
		catch (Throwable e)
		{
			aReadLock.unlock();

			throw e;
		}
	}


	private synchronized BlockAccessor getBlockAccessor() throws IOException
	{
		return new BlockAccessor(mDatabase.getBlockDevice(), mDatabase.getCompressionParameter(), 0);
	}
}

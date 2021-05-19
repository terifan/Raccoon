package org.terifan.raccoon;

import org.terifan.raccoon.io.DatabaseIOException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.terifan.raccoon.storage.BlockAccessor;
import org.terifan.raccoon.serialization.Marshaller;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;


public final class TableInstance<T>
{
	public final static byte TYPE_DEFAULT = 0;
	public final static byte TYPE_BLOB = 1;

	private final Database mDatabase;
	private final Table mTable;
	private final HashSet<CommitLock> mCommitLocks;
	private final ITableImplementation mTableImplementation;
	private final Cost mCost;


	TableInstance(Database aDatabase, Table aTable, byte[] aTableHeader)
	{
		mCost = new Cost();
		mCommitLocks = new HashSet<>();

		mDatabase = aDatabase;
		mTable = aTable;

		CompressionParam compression = mDatabase.getCompressionParameter();
		TableParam parameter = mDatabase.getTableParameter();

		mTableImplementation = new ExtendibleHashTable();
		if (aTableHeader == null)
		{
			mTableImplementation.create(mDatabase.getBlockDevice(), mDatabase.getTransactionId(), false, compression, parameter, aTable.getTypeName(), mCost, aDatabase.getPerformanceTool());
		}
		else
		{
			mTableImplementation.open(mDatabase.getBlockDevice(), mDatabase.getTransactionId(), false, compression, parameter, aTable.getTypeName(), mCost, aDatabase.getPerformanceTool(), aTableHeader);
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


	public boolean get(T aEntity, boolean aFetchLazy)
	{
		Log.i("get entity %s", aEntity);
		Log.inc();

		try
		{
			ArrayMapEntry entry = new ArrayMapEntry(getKeys(aEntity));

			if (mTableImplementation.get(entry))
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


	/**
	 * Saves an entity.
	 *
	 * @return
	 * true if this table did not already contain the specified entity
	 */
	public boolean save(T aEntity)
	{
		Log.i("save %s", aEntity.getClass());
		Log.inc();

		byte[] key = getKeys(aEntity);
		byte[] value = getNonKeys(aEntity);
		byte type = TYPE_DEFAULT;

		if (key.length + value.length + 1 > mTableImplementation.getEntrySizeLimit())
		{
			type = TYPE_BLOB;

			try (LobByteChannelImpl blob = new LobByteChannelImpl(getBlockAccessor(), mDatabase.getTransactionId(), null, LobOpenOption.WRITE))
			{
				blob.write(ByteBuffer.wrap(value));
				value = blob.finish();
			}
			catch (IOException e)
			{
				throw new DatabaseException(e);
			}
		}

		ArrayMapEntry entry = new ArrayMapEntry(key, value, type);

		ArrayMapEntry oldEntry = mTableImplementation.put(entry);

		if (oldEntry != null)
		{
			deleteIfBlob(oldEntry);
		}

		Log.dec();

		return oldEntry == null;
	}


	private void deleteIfBlob(ArrayMapEntry aEntry)
	{
		if (aEntry.getType() == TYPE_BLOB)
		{
			LobByteChannelImpl.deleteBlob(getBlockAccessor(), aEntry.getValue());
		}
	}


	public LobByteChannelImpl openBlob(T aEntityKey, LobOpenOption aOpenOption)
	{
		try
		{
			CommitLock lock = new CommitLock();

			byte[] key = getKeys(aEntityKey);
			ArrayMapEntry entry = new ArrayMapEntry(key);

			byte[] header;

			if (mTableImplementation.get(entry))
			{
				if (entry.getType() != TYPE_BLOB)
				{
					throw new IllegalArgumentException("Not a blob");
				}

				if (aOpenOption == LobOpenOption.CREATE)
				{
					throw new IllegalArgumentException("A blob already exists with this key.");
				}

				if (aOpenOption == LobOpenOption.REPLACE)
				{
					deleteIfBlob(entry);

					header = null;
				}
				else
				{
					header = entry.getValue();
				}
			}
			else
			{
				header = null;
			}

			LobByteChannelImpl out = new LobByteChannelImpl(getBlockAccessor(), mDatabase.getTransactionId(), header, aOpenOption)
			{
				@Override
				public void close()
				{
					try
					{
						try
						{
							if (isModified())
							{
								Log.d("write blob entry");

								byte[] header = finish();

								mDatabase.aquireWriteLock();
								try
								{
									ArrayMapEntry entry = new ArrayMapEntry(key, header, TYPE_BLOB);
									mTableImplementation.put(entry);
								}
								catch (DatabaseException e)
								{
									mDatabase.forceClose(e);
									throw e;
								}
								finally
								{
									mDatabase.releaseWriteLock();
								}
							}
						}
						finally
						{
							synchronized (TableInstance.this)
							{
								mCommitLocks.remove(lock);
							}

							super.close();
						}
					}
					catch (IOException e)
					{
						throw new DatabaseIOException(e);
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


	public boolean remove(T aEntity)
	{
		ArrayMapEntry entry = new ArrayMapEntry(getKeys(aEntity));

		ArrayMapEntry oldEntry = mTableImplementation.remove(entry);

		if (oldEntry != null)
		{
			deleteIfBlob(oldEntry);

			return true;
		}

		return false;
	}


	/**
	 * Creates an iterator over all items in this table. This iterator will reconstruct entities.
	 */
	public Iterator<T> iterator()
	{
		return new EntityIterator(this, getEntryIterator());
	}


	Iterator<ArrayMapEntry> getEntryIterator()
	{
		return mTableImplementation.iterator();
	}


	public void clear()
	{
		mTableImplementation.removeAll(this::deleteIfBlob);
	}


	public void close()
	{
		mTableImplementation.close();
	}


	boolean isModified()
	{
		return mTableImplementation.isChanged();
	}


	boolean commit()
	{
		synchronized (this)
		{
			if (!mCommitLocks.isEmpty())
			{
				StringBuilder sb = new StringBuilder();
				for (CommitLock cl : mCommitLocks)
				{
					sb.append("\nCaused by calling method: " + cl.getOwner());
				}

				throw new CommitBlockedException("A table cannot be committed while a stream is open." + sb.toString());
			}
		}

		AtomicBoolean changed = new AtomicBoolean();
		byte[] newPointer = mTableImplementation.commit(changed);

		if (!changed.get())
		{
			return false;
		}

		if (Arrays.equals(newPointer, mTable.getTableHeader()))
		{
			return false;
		}

		mTable.setTableHeader(newPointer);

		return true;
	}


	void rollback()
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


	void unmarshalToObjectKeys(ArrayMapEntry aBuffer, Object aOutput)
	{
		ByteArrayBuffer buffer = ByteArrayBuffer.wrap(aBuffer.getKey());

		mTable.getMarshaller().unmarshal(buffer, aOutput, Table.FIELD_CATEGORY_KEY);

		mCost.mUnmarshalKeys++;
	}


	void unmarshalToObjectValues(ArrayMapEntry aBuffer, Object aOutput)
	{
		ByteArrayBuffer buffer;

		if (aBuffer.getType() == TYPE_BLOB)
		{
			try (LobByteChannelImpl blob = new LobByteChannelImpl(getBlockAccessor(), mDatabase.getTransactionId(), aBuffer.getValue(), LobOpenOption.READ))
			{
				byte[] buf = new byte[(int)blob.size()];
				blob.read(ByteBuffer.wrap(buf));
				buffer = ByteArrayBuffer.wrap(buf);
			}
			catch (IOException e)
			{
				throw new DatabaseException(e);
			}
		}
		else
		{
			buffer = ByteArrayBuffer.wrap(aBuffer.getValue(), false);
		}

		Marshaller marshaller = mTable.getMarshaller();
		marshaller.unmarshal(buffer, aOutput, Table.FIELD_CATEGORY_DISCRIMINATOR | Table.FIELD_CATEGORY_VALUE);

		mCost.mUnmarshalValues++;
	}


	private byte[] getKeys(Object aInput)
	{
		return mTable.getMarshaller().marshal(ByteArrayBuffer.alloc(16), aInput, Table.FIELD_CATEGORY_KEY).trim().array();
	}


	private byte[] getNonKeys(Object aInput)
	{
		ByteArrayBuffer buffer = ByteArrayBuffer.alloc(16);

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
		mTableImplementation.scan(aScanResult);
	}


	<T> Stream<T> stream(Lock aReadLock)
	{
		try
		{
			Stream<T> tmp = StreamSupport.stream(new Spliterators.AbstractSpliterator<T>(Long.MAX_VALUE, Spliterator.IMMUTABLE | Spliterator.NONNULL)
			{
				EntityIterator entityIterator = new EntityIterator(TableInstance.this, mTableImplementation.iterator());

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


	private synchronized BlockAccessor getBlockAccessor()
	{
		return new BlockAccessor(mDatabase.getBlockDevice(), mDatabase.getCompressionParameter());
	}
}

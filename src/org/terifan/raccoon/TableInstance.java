package org.terifan.raccoon;

import org.terifan.raccoon.io.DatabaseIOException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
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

	private final Table mTable;
	private final HashSet<CommitLock> mCommitLocks;
	private final TableImplementation mTableImplementation;


	TableInstance(Database mDatabase, Table aTable, byte[] aTableHeader)
	{
		mCommitLocks = new HashSet<>();

		mTable = aTable;

		try
		{
			Class<? extends TableImplementation> type;

			if ("btree".equals(aTable.getImplementation()))
			{
				type = BTreeTableImplementation.class;
			}
			else if ("hashtable".equals(aTable.getImplementation()))
			{
				type = ExtendibleHashTableImplementation.class;
			}
			else
			{
				throw new IllegalArgumentException("No supported table implementation: " + aTable.getImplementation());
			}

			mTableImplementation = (TableImplementation)type.getDeclaredConstructors()[0].newInstance(mDatabase.getBlockDevice(), mDatabase.getTransactionGroup(), false, mDatabase.getCompressionParameter(), mDatabase.getTableParameter(), aTable.getEntityName());
		}
		catch (IllegalAccessException | InstantiationException | SecurityException | InvocationTargetException e)
		{
			throw new IllegalArgumentException(e);
		}

		mTableImplementation.openOrCreateTable(aTableHeader);
	}


	public boolean get(Database mDatabase, T aEntity)
	{
		Log.i("get entity %s", aEntity);
		Log.inc();

		try
		{
			ArrayMapEntry entry = new ArrayMapEntry(getKeys(aEntity));

			if (mTableImplementation.get(entry))
			{
				TransactionGroup tx = mDatabase.getTransactionGroup();

				unmarshalToObjectValues(mDatabase, entry, aEntity, tx);

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


	public <T> List<T> list(Database mDatabase, Class<T> aType, int aLimit)
	{
		ArrayList<T> list = new ArrayList<>();
		for (Iterator<T> it = (Iterator<T>)iterator(mDatabase); list.size() < aLimit && it.hasNext();)
		{
			list.add(it.next());
		}
		return list;
	}


	/**
	 * Saves an entity.
	 *
	 * @return
	 * true if this table did not already contain the specified entity
	 */
	public boolean save(Database mDatabase, T aEntity)
	{
		Log.i("save %s", aEntity.getClass());
		Log.inc();

		byte[] key = getKeys(aEntity);
		byte[] value = getNonKeys(aEntity);
		byte type = TYPE_DEFAULT;

		if (key.length + value.length + 1 > mTableImplementation.getEntrySizeLimit())
		{
			type = TYPE_BLOB;

			try (LobByteChannelImpl blob = new LobByteChannelImpl(getBlockAccessor(mDatabase), mDatabase.getTransactionGroup(), null, LobOpenOption.WRITE))
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
			deleteIfBlob(mDatabase, oldEntry);
		}

		Log.dec();

		return oldEntry == null;
	}


	private void deleteIfBlob(Database mDatabase, ArrayMapEntry aEntry)
	{
		if (aEntry.getType() == TYPE_BLOB)
		{
			LobByteChannelImpl.deleteBlob(getBlockAccessor(mDatabase), aEntry.getValue());
		}
	}


	public LobByteChannelImpl openBlob(Database mDatabase, T aEntityKey, LobOpenOption aOpenOption)
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
					deleteIfBlob(mDatabase, entry);

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

			LobByteChannelImpl out = new LobByteChannelImpl(getBlockAccessor(mDatabase), mDatabase.getTransactionGroup(), header, aOpenOption)
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


	public boolean remove(Database mDatabase, T aEntity)
	{
		ArrayMapEntry entry = new ArrayMapEntry(getKeys(aEntity));

		ArrayMapEntry oldEntry = mTableImplementation.remove(entry);

		if (oldEntry != null)
		{
			deleteIfBlob(mDatabase, oldEntry);

			return true;
		}

		return false;
	}


	/**
	 * Creates an iterator over all items in this table. This iterator will reconstruct entities.
	 */
	public Iterator<T> iterator(Database mDatabase)
	{
		return new EntityIterator(mDatabase, this, getEntryIterator());
	}


	Iterator<ArrayMapEntry> getEntryIterator()
	{
		return mTableImplementation.iterator();
	}


	public void clear(Database mDatabase)
	{
		mTableImplementation.removeAll(e -> deleteIfBlob(mDatabase, e));
	}


	public void close()
	{
		mTableImplementation.close();
	}


	boolean isModified()
	{
		return mTableImplementation.isChanged();
	}


	long flush(TransactionGroup aTransactionGroup)
	{
		return mTableImplementation.flush(aTransactionGroup);
	}


	boolean commit(TransactionGroup aTransactionGroup)
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
		byte[] newPointer = mTableImplementation.commit(aTransactionGroup, changed);

		if (!changed.get() || Arrays.equals(newPointer, mTable.getTableHeader()))
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

		mTable.getMarshaller().unmarshal(buffer, aOutput, Table.FIELD_CATEGORY_ID);
	}


	void unmarshalToObjectValues(Database mDatabase, ArrayMapEntry aBuffer, Object aOutput, TransactionGroup aTransactionGroup)
	{
		ByteArrayBuffer buffer;

		if (aBuffer.getType() == TYPE_BLOB)
		{
			try (LobByteChannelImpl blob = new LobByteChannelImpl(getBlockAccessor(mDatabase), aTransactionGroup, aBuffer.getValue(), LobOpenOption.READ))
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
	}


	private byte[] getKeys(Object aInput)
	{
		return mTable.getMarshaller().marshal(ByteArrayBuffer.alloc(16), aInput, Table.FIELD_CATEGORY_ID).trim().array();
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


	<T> Stream<T> stream(Database mDatabase, Lock aReadLock)
	{
		try
		{
			Stream<T> tmp = StreamSupport.stream(new Spliterators.AbstractSpliterator<T>(Long.MAX_VALUE, Spliterator.IMMUTABLE | Spliterator.NONNULL)
			{
				EntityIterator entityIterator = new EntityIterator(mDatabase, TableInstance.this, mTableImplementation.iterator());

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


	private synchronized BlockAccessor getBlockAccessor(Database mDatabase)
	{
		return new BlockAccessor(mDatabase.getBlockDevice(), mDatabase.getCompressionParameter());
	}
}

package org.terifan.raccoon;

import org.terifan.raccoon.btree.BTree;
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
import org.terifan.bundle.Document;
import org.terifan.raccoon.btree.ArrayMapEntry;
import org.terifan.raccoon.storage.BlockAccessor;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;


public final class TableInstance<T>
{
	public final static byte TYPE_DEFAULT = 0;
	public final static byte TYPE_BLOB = 1;

	private final Table mTable;
	private final HashSet<CommitLock> mCommitLocks;
	private final BTree mTableImplementation;


	TableInstance(Database mDatabase, Table aTable, byte[] aTableHeader)
	{
		mCommitLocks = new HashSet<>();

		mTable = aTable;

		mTableImplementation = new BTree(mDatabase.getBlockDevice(), mDatabase.getTransactionGroup(), false, mDatabase.getCompressionParameter(), mDatabase.getTableParameter(), aTable.getEntityName());

		mTableImplementation.openOrCreateTable(aTableHeader);
	}


	public boolean get(Database aDatabase, Document aDocument)
	{
		Log.i("get entity %s", aDocument);
		Log.inc();

		try
		{
			ArrayMapEntry entry = new ArrayMapEntry(getKeys(aDocument));

			if (mTableImplementation.get(entry))
			{
				TransactionGroup tx = aDatabase.getTransactionGroup();

				unmarshalToObjectValues(aDatabase, entry, aDocument, tx);

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
	public boolean save(Database aDatabase, Document aDocument) throws IOException
	{
		Log.i("save %s", aDocument.getClass());
		Log.inc();

		byte[] key = aDocument.getString("_id").getBytes();
		byte[] value = aDocument.marshal();
		byte type = TYPE_DEFAULT;

		if (key.length + value.length + 1 > mTableImplementation.getEntrySizeLimit())
		{
			type = TYPE_BLOB;

			try (LobByteChannelImpl blob = new LobByteChannelImpl(getBlockAccessor(aDatabase), aDatabase.getTransactionGroup(), null, LobOpenOption.WRITE))
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
			deleteIfBlob(aDatabase, oldEntry);
		}

		Log.dec();

		return oldEntry == null;
	}


	private void deleteIfBlob(Database aDatabase, ArrayMapEntry aEntry)
	{
		if (aEntry.getType() == TYPE_BLOB)
		{
			LobByteChannelImpl.deleteBlob(getBlockAccessor(aDatabase), aEntry.getValue());
		}
	}


	public LobByteChannelImpl openBlob(Database aDatabase, T aEntityKey, LobOpenOption aOpenOption)
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
					deleteIfBlob(aDatabase, entry);

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

			LobByteChannelImpl out = new LobByteChannelImpl(getBlockAccessor(aDatabase), aDatabase.getTransactionGroup(), header, aOpenOption)
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

								aDatabase.aquireWriteLock();
								try
								{
									ArrayMapEntry entry = new ArrayMapEntry(key, header, TYPE_BLOB);
									mTableImplementation.put(entry);
								}
								catch (DatabaseException e)
								{
									aDatabase.forceClose(e);
									throw e;
								}
								finally
								{
									aDatabase.releaseWriteLock();
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


	public boolean remove(Database aDatabase, Document aDocument)
	{
		ArrayMapEntry entry = new ArrayMapEntry(getKeys(aDocument));

		ArrayMapEntry oldEntry = mTableImplementation.remove(entry);

		if (oldEntry != null)
		{
			deleteIfBlob(aDatabase, oldEntry);

			return true;
		}

		return false;
	}


	/**
	 * Creates an iterator over all items in this table. This iterator will reconstruct entities.
	 */
	public Iterator<T> iterator(Database aDatabase)
	{
		return new DocumentIterator(aDatabase, this);
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


	void scan(ScanResult aScanResult)
	{
		mTableImplementation.scan(aScanResult);
	}


//	<T> Stream<T> stream(Database mDatabase, Lock aReadLock)
//	{
//		try
//		{
//			Stream<T> tmp = StreamSupport.stream(new Spliterators.AbstractSpliterator<T>(Long.MAX_VALUE, Spliterator.IMMUTABLE | Spliterator.NONNULL)
//			{
//				EntityIterator entityIterator = new EntityIterator(mDatabase, TableInstance.this, mTableImplementation.iterator());
//
//				@Override
//				public boolean tryAdvance(Consumer<? super T> aConsumer)
//				{
//					if (!entityIterator.hasNext())
//					{
//						aReadLock.unlock();
//
//						return false;
//					}
//					aConsumer.accept((T)entityIterator.next());
//					return true;
//				}
//			}, false);
//
//			return tmp;
//		}
//		catch (Throwable e)
//		{
//			aReadLock.unlock();
//
//			throw e;
//		}
//	}


	private synchronized BlockAccessor getBlockAccessor(Database mDatabase)
	{
		return new BlockAccessor(mDatabase.getBlockDevice(), mDatabase.getCompressionParameter());
	}
}

package org.terifan.raccoon;

import org.terifan.raccoon.btree.BTree;
import org.terifan.raccoon.io.DatabaseIOException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.terifan.bundle.Document;
import org.terifan.raccoon.btree.ArrayMapEntry;
import org.terifan.raccoon.storage.BlockAccessor;
import org.terifan.raccoon.util.ByteArrayUtil;
import org.terifan.raccoon.util.Log;


public final class TableInstance
{
	public final static byte TYPE_DEFAULT = 0;
	public final static byte TYPE_BLOB = 1;

	private final Database mDatabase;
	private final String mName;
	private final HashSet<CommitLock> mCommitLocks;
	private final BTree mTableImplementation;
	private Document mTableHeader;


	TableInstance(Database aDatabase, String aName, Document aTableHeader)
	{
		mCommitLocks = new HashSet<>();

		mTableHeader = aTableHeader;
		mDatabase = aDatabase;
		mName = aName;

		mTableImplementation = new BTree(aDatabase.getBlockDevice(), aDatabase.getTransactionGroup(), false);
		mTableImplementation.openOrCreateTable(aName, aTableHeader);
	}


	public boolean tryGet(Document aDocument)
	{
		return get(aDocument) != null;
	}


	public Document get(Document aDocument)
	{
		Log.i("get entity %s", aDocument);
		Log.inc();

		try
		{
			ArrayMapEntry entry = new ArrayMapEntry(getKey(aDocument));

			if (mTableImplementation.get(entry))
			{
				return Document.unmarshal(entry.getValue());

//				TransactionGroup tx = mDatabase.getTransactionGroup();
//				unmarshalToObjectValues(mDatabase, entry, aDocument, tx);
			}
		}
		finally
		{
			Log.dec();
		}

		return null;
	}


	public List<Document> list(int aLimit)
	{
		ArrayList<Document> list = new ArrayList<>();
		for (Iterator<Document> it = iterator(); list.size() < aLimit && it.hasNext();)
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
	public boolean save(Document aDocument)
	{
		Log.i("save %s", aDocument.getClass());
		Log.inc();

		byte[] key = getKey(aDocument);
		byte[] value = aDocument.marshal();
		byte type = TYPE_DEFAULT;

//		if (key.length + value.length + 1 > mTableImplementation.getEntrySizeLimit())
//		{
//			type = TYPE_BLOB;
//
//			try (LobByteChannelImpl blob = new LobByteChannelImpl(getBlockAccessor(mDatabase), mDatabase.getTransactionGroup(), null, LobOpenOption.WRITE))
//			{
//				blob.write(ByteBuffer.wrap(value));
//				value = blob.finish();
//			}
//			catch (IOException e)
//			{
//				throw new DatabaseException(e);
//			}
//		}

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
			LobByteChannelImpl.deleteBlob(getBlockAccessor(mDatabase), aEntry.getValue());
		}
	}


	public LobByteChannelImpl openBlob(Document aEntityKey, LobOpenOption aOpenOption)
	{
		try
		{
			CommitLock lock = new CommitLock();

			byte[] key = getKey(aEntityKey);
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

//								mDatabase.aquireWriteLock();
//								try
//								{
									ArrayMapEntry entry = new ArrayMapEntry(key, header, TYPE_BLOB);
									mTableImplementation.put(entry);
//								}
//								catch (DatabaseException e)
//								{
//									mDatabase.forceClose(e);
//									throw e;
//								}
//								finally
//								{
//									mDatabase.releaseWriteLock();
//								}
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


	public boolean remove(Document aDocument)
	{
		ArrayMapEntry entry = new ArrayMapEntry(getKey(aDocument));

		ArrayMapEntry oldEntry = mTableImplementation.remove(entry);

		if (oldEntry != null)
		{
//			deleteIfBlob(mDatabase, oldEntry);

			return true;
		}

		return false;
	}


	/**
	 * Creates an iterator over all items in this table. This iterator will reconstruct entities.
	 */
	public Iterator<Document> iterator()
	{
		return new DocumentIterator(this, getEntryIterator());
	}


	Iterator<ArrayMapEntry> getEntryIterator()
	{
		return mTableImplementation.iterator();
	}


	public void clear()
	{
		getEntryIterator().forEachRemaining(entry -> {
			deleteIfBlob(entry);
			mTableImplementation.remove(entry);
		});
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
		Document tableHeader = mTableImplementation.commit(mDatabase.getTransactionGroup(), changed);

		if (!changed.get() || mTableHeader.equals(tableHeader))
		{
			return false;
		}

		mTableHeader = tableHeader;

		return true;
	}


	public Document getTableHeader()
	{
		return mTableHeader;
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


//	void unmarshalToObjectValues(ArrayMapEntry aEntry, TransactionGroup aTransactionGroup)
//	{
//		ByteArrayBuffer buffer;
//
//		if (aEntry.getType() == TYPE_BLOB)
//		{
//			try (LobByteChannelImpl blob = new LobByteChannelImpl(getBlockAccessor(mDatabase), aTransactionGroup, aEntry.getValue(), LobOpenOption.READ))
//			{
//				byte[] buf = new byte[(int)blob.size()];
//				blob.read(ByteBuffer.wrap(buf));
//				buffer = ByteArrayBuffer.wrap(buf);
//			}
//			catch (IOException e)
//			{
//				throw new DatabaseException(e);
//			}
//		}
//		else
//		{
//			buffer = ByteArrayBuffer.wrap(aEntry.getValue(), false);
//		}
//
//		Marshaller marshaller = mName.getMarshaller();
//		marshaller.unmarshal(buffer, aOutput, Table.FIELD_CATEGORY_DISCRIMINATOR | Table.FIELD_CATEGORY_VALUE);
//	}


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


	private byte[] getKey(Document aDocument)
	{
		byte[] buf = new byte[8];
		ByteArrayUtil.putInt64(buf, 0, aDocument.getNumber("_id").longValue());
//		Log.hexDump(buf);
		return buf;
	}
}

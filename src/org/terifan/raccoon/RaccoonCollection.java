package org.terifan.raccoon;

import org.terifan.raccoon.btree.BTree;
import org.terifan.raccoon.io.DatabaseIOException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import org.terifan.bundle.Document;
import org.terifan.raccoon.btree.ArrayMapEntry;
import org.terifan.raccoon.btree.BTreeEntryIterator;
import org.terifan.raccoon.btree.BTreeStorage;
import org.terifan.raccoon.storage.BlockAccessor;
import org.terifan.raccoon.util.ByteArrayUtil;
import org.terifan.raccoon.util.Log;


public final class RaccoonCollection implements BTreeStorage
{
	public final static byte TYPE_TREENODE = 0;
	public final static byte TYPE_DOCUMENT = 1;
	public final static byte TYPE_EXTERNAL = 2;

	private final RaccoonDatabase mDatabase;
	private final HashSet<CommitLock> mCommitLocks;
	private final BTree mTableImplementation;
	private final Document mConfiguration;


	RaccoonCollection(RaccoonDatabase aDatabase, Document aConfiguration)
	{
		mCommitLocks = new HashSet<>();

		mConfiguration = aConfiguration;
		mDatabase = aDatabase;

		mTableImplementation = new BTree((BTreeStorage)this, mConfiguration);
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
		Log.i("save %s", aDocument);
		Log.inc();

		byte[] key = getKey(aDocument);
		byte[] value = aDocument.marshal();
		byte type = TYPE_DOCUMENT;

//		if (key.length + value.length + 1 > mConfiguration.getInt("entrySizeLimit"))
//		{
//			type = TYPE_EXTERNAL;
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
		if (aEntry.getType() == TYPE_EXTERNAL)
		{
			LobByteChannelImpl.deleteBlob(mDatabase.getBlockAccessor(), aEntry.getValue());
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
				if (entry.getType() != TYPE_EXTERNAL)
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

			LobByteChannelImpl out = new LobByteChannelImpl(mDatabase, header, aOpenOption)
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
									ArrayMapEntry entry = new ArrayMapEntry(key, header, TYPE_EXTERNAL);
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
							synchronized (RaccoonCollection.this)
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
	 * Creates an iterator over all items in this table.
	 */
	public DocumentIterator iterator()
	{
		return new DocumentIterator(getEntryIterator());
	}


	BTreeEntryIterator getEntryIterator()
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


	@Override
	public void close()
	{
		mTableImplementation.close();
	}


	boolean isModified()
	{
		return mTableImplementation.isChanged();
	}


	long flush()
	{
		return mTableImplementation.flush();
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

		return mTableImplementation.commit();
	}


	public Document getConfiguration()
	{
		return mConfiguration;
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


	private byte[] getKey(Document aDocument)
	{
//		byte[] buf = new byte[8];
//		ByteArrayUtil.putInt64(buf, 0, aDocument.getNumber("_id").longValue());
		byte[] buf = String.format("%08d", aDocument.getNumber("_id").longValue()).getBytes();
		return buf;
	}


	@Override
	public BlockAccessor getBlockAccessor()
	{
		return new BlockAccessor(mDatabase.getBlockDevice(), mDatabase.getCompressionParameter());
	}


	@Override
	public long getTransaction()
	{
		return mDatabase.getTransaction();
	}


	public BTree getTableImplementation()
	{
		return mTableImplementation;
	}
}

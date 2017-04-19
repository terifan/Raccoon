package org.terifan.raccoon;

import org.terifan.raccoon.hashtable.HashTable;
import org.terifan.raccoon.hashtable.LeafEntry;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
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
import org.terifan.raccoon.storage.Blob;
import org.terifan.raccoon.storage.BlobInputStream;
import org.terifan.raccoon.storage.BlobOutputStream;
import org.terifan.raccoon.storage.BlockAccessor;
import org.terifan.raccoon.serialization.Marshaller;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;


public final class TableType<T> implements Iterable<T>, AutoCloseable
{
	public static final byte FLAG_BLOB = 1;

	private Database mDatabase;
	private Table mTableMetadata;
	private BlockAccessor mBlockAccessor;
	private HashSet<BlobOutputStream> mOpenOutputStreams;
	private HashTable mTableImplementation;
	private byte[] mPointer;


	TableType(Database aDatabase, Table aTableMetadata, byte[] aPointer)
	{
		try
		{
			mOpenOutputStreams = new HashSet<>();

			mDatabase = aDatabase;
			mTableMetadata = aTableMetadata;
			mPointer = aPointer;

			mTableImplementation = new HashTable(mDatabase.getBlockDevice(), mPointer, mDatabase.getTransactionId(), false, mDatabase.getParameter(CompressionParam.class, null), mDatabase.getParameter(TableParam.class, null));

			mBlockAccessor = new BlockAccessor(mDatabase.getBlockDevice());

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

		LeafEntry entry = new LeafEntry(getKeys(aEntity));

		if (mTableImplementation.get(entry))
		{
			unmarshalToObjectValues(entry, aEntity);

			Log.dec();

			return true;
		}

		Log.dec();

		return false;
	}


	public <T> List<T> list(Class<T> aType)
	{
		ArrayList<T> list = new ArrayList<>();
		iterator().forEachRemaining(e -> list.add((T)e));
		return list;
	}


	public synchronized InputStream read(T aEntity)
	{
		LeafEntry entry = new LeafEntry(getKeys(aEntity));

		if (!mTableImplementation.get(entry))
		{
			return null;
		}

		if (entry.hasFlag(FLAG_BLOB))
		{
			try
			{
				ByteArrayBuffer buffer = new ByteArrayBuffer(entry.getValue());

				return new BlobInputStream(mBlockAccessor, buffer);
			}
			catch (Exception e)
			{
				throw new DatabaseException(e);
			}
		}

		return new ByteArrayInputStream(entry.getValue());
	}


	public boolean save(T aEntity)
	{
		Log.i("save entity");
		Log.inc();

		byte[] key = getKeys(aEntity);
		byte[] value = getNonKeys(aEntity);
		byte type = 0;

		if (key.length + value.length > mTableImplementation.getEntryMaximumLength() / 4)
		{
			type = FLAG_BLOB;

			try (BlobOutputStream bos = new BlobOutputStream(mBlockAccessor, mDatabase.getTransactionId(), null))
			{
				bos.write(value);
				value = bos.finish();
			}
			catch (IOException e)
			{
				throw new DatabaseException(e);
			}
		}

		LeafEntry entry = new LeafEntry(key, value, type);

		if (mTableImplementation.put(entry))
		{
			deleteIfBlob(entry);
		}

		Log.dec();

		return entry.getValue() != null;
	}


	private void deleteIfBlob(LeafEntry aEntry) throws DatabaseException
	{
		if (aEntry.hasFlag(FLAG_BLOB))
		{
			try
			{
				Blob.deleteBlob(mBlockAccessor, aEntry.getValue());
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
			BlobOutputStream.OnCloseListener onCloseListener = new BlobOutputStream.OnCloseListener()
			{
				@Override
				public void onClose(BlobOutputStream aBlobOutputStream, byte[] aHeader)
				{
					Log.v("write blob entry");

					byte[] key = getKeys(aEntityKey);

					LeafEntry entry = new LeafEntry(key, aHeader, FLAG_BLOB);

					if (mTableImplementation.put(entry))
					{
						deleteIfBlob(entry);
					}

					synchronized (TableType.this)
					{
						mOpenOutputStreams.remove(aBlobOutputStream);
					}
				}
			};

			BlobOutputStream out = new BlobOutputStream(mBlockAccessor, mDatabase.getTransactionId(), onCloseListener);

			synchronized (this)
			{
				mOpenOutputStreams.add(out);
			}

			return out;
		}
		catch (IOException e)
		{
			throw new DatabaseIOException(e);
		}
	}


	public boolean save(T aEntity, InputStream aInputStream)
	{
		try (BlobOutputStream bos = new BlobOutputStream(mBlockAccessor, mDatabase.getTransactionId(), null))
		{
			bos.write(Streams.readAll(aInputStream));

			byte[] key = getKeys(aEntity);

			LeafEntry entry = new LeafEntry(key, bos.finish(), FLAG_BLOB);

			if (mTableImplementation.put(entry))
			{
				deleteIfBlob(entry);
			}

			return entry.getValue() == null;
		}
		catch (IOException e)
		{
			throw new DatabaseException(e);
		}
	}


	public boolean remove(T aEntity)
	{
		LeafEntry entry = new LeafEntry(getKeys(aEntity));

		if (mTableImplementation.remove(entry))
		{
			deleteIfBlob(entry);

			return true;
		}

		return false;
	}


	/**
	 * Creates an iterator over all items in this table. This iterator will reconstruct entities.
	 */
	@Override
	public Iterator<T> iterator()
	{
		return new EntityIterator(this, getLeafIterator());
	}


	Iterator<LeafEntry> getLeafIterator()
	{
		return mTableImplementation.iterator();
	}


	/**
	 * Creates an iterator over all items in this table.
	 */
//	public Iterator<ResultSet> resultSetIterator()
//	{
//		return new ResultSetIterator(this, mTableImplementation.iterator());
//	}


	public void clear()
	{
		mTableImplementation.clear();
	}


	/**
	 * Clean-up resources only
	 */
	@Override
	public void close() throws IOException
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

		byte[] newPointer = mTableImplementation.marshalHeader();

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


	void unmarshalToObjectKeys(LeafEntry aBuffer, Object aOutput)
	{
		ByteArrayBuffer buffer = new ByteArrayBuffer(aBuffer.getKey());

		mTableMetadata.getMarshaller().unmarshal(buffer, aOutput, Table.FIELD_CATEGORY_KEY);
	}


	void unmarshalToObjectValues(LeafEntry aBuffer, Object aOutput)
	{
		ByteArrayBuffer buffer = new ByteArrayBuffer(aBuffer.getValue());

		if (aBuffer.hasFlag(FLAG_BLOB))
		{
			try
			{
				buffer.wrap(Streams.readAll(new BlobInputStream(mBlockAccessor, buffer)));
				buffer.position(0);
			}
			catch (Exception e)
			{
				throw new DatabaseException(e);
			}
		}

		Marshaller marshaller = mTableMetadata.getMarshaller();
		marshaller.unmarshal(buffer, aOutput, Table.FIELD_CATEGORY_DISCRIMINATOR | Table.FIELD_CATEGORY_VALUE);
	}


	private byte[] getKeys(Object aInput)
	{
		return mTableMetadata.getMarshaller().marshal(new ByteArrayBuffer(16), aInput, Table.FIELD_CATEGORY_KEY).trim().array();
	}


	private byte[] getNonKeys(Object aInput)
	{
		ByteArrayBuffer buffer = new ByteArrayBuffer(16);

		Marshaller marshaller = mTableMetadata.getMarshaller();
		marshaller.marshal(buffer, aInput, Table.FIELD_CATEGORY_DISCRIMINATOR | Table.FIELD_CATEGORY_VALUE);

		return buffer.trim().array();
	}


	public Table getTable()
	{
		return mTableMetadata;
	}


	@Override
	public String toString()
	{
		return mTableMetadata.toString();
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
				EntityIterator entityIterator = new EntityIterator(TableType.this, mTableImplementation.iterator());

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


	public class TypeResult
	{
		public byte type;
	}
}

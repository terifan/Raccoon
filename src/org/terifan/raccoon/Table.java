package org.terifan.raccoon;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import org.terifan.raccoon.io.Blob;
import org.terifan.raccoon.io.BlobInputStream;
import org.terifan.raccoon.io.BlobOutputStream;
import org.terifan.raccoon.io.BlockAccessor;
import org.terifan.raccoon.io.Streams;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;


public class Table<T> implements Iterable<T>
{
	static final byte PTR_BLOB = 1;

	private Database mDatabase;
	private TableMetadata mTableMetadata;
	private BlockAccessor mBlockAccessor;
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

			mTableImplementation = new HashTable(mDatabase.getBlockDevice(), mPointer, mDatabase.getTransactionId(), false, mDatabase.getParameter(CompressionParam.class, null));

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
			unmarshalToObjectValues(aEntity, entry);

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

		ByteArrayBuffer buffer = new ByteArrayBuffer(entry.getValue());

		if (entry.getFormat() == PTR_BLOB)
		{
			try
			{
				return new BlobInputStream(mBlockAccessor, buffer);
			}
			catch (Exception e)
			{
				throw new DatabaseException(e);
			}
		}

		return buffer;
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
		if (aEntry.getFormat() == PTR_BLOB)
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
			BlobOutputStream out = new BlobOutputStream(mBlockAccessor, mDatabase.getTransactionId());

			synchronized (this)
			{
				mOpenOutputStreams.add(out);
			}

			out.setOnCloseListener((aHeader)->
			{
				Log.v("write blob entry");

				byte[] key = getKeys(aEntityKey);

				LeafEntry entry = new LeafEntry(key, aHeader, PTR_BLOB);

				if (mTableImplementation.put(entry))
				{
					deleteIfBlob(entry);
				}

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


	public boolean save(T aEntity, InputStream aInputStream)
	{
		try (BlobOutputStream bos = new BlobOutputStream(mBlockAccessor, mDatabase.getTransactionId()))
		{
			bos.write(Streams.readAll(aInputStream));

			byte[] key = getKeys(aEntity);

			LeafEntry entry = new LeafEntry(key, bos.finish(), PTR_BLOB);

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


	@Override
	public Iterator<T> iterator()
	{
		return new EntityIterator(this, mTableImplementation.iterator());
	}


	public Iterator<LeafEntry> iteratorRaw()
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


	void unmarshalToObjectKeys(Object aOutput, LeafEntry aMarshalledData)
	{
		ByteArrayBuffer buffer = new ByteArrayBuffer(aMarshalledData.getKey());

		mTableMetadata.getMarshaller().unmarshalKeys(buffer, aOutput);
	}


	void unmarshalToObjectValues(Object aOutput, LeafEntry aMarshalledData)
	{
		ByteArrayBuffer buffer = new ByteArrayBuffer(aMarshalledData.getValue());

		if (aMarshalledData.getFormat() == PTR_BLOB)
		{
			try
			{
				buffer.wrap(Streams.readAll(new BlobInputStream(mBlockAccessor, buffer)));
				buffer.position(0);
			}
			catch (Exception e)
			{
				Log.dec();

				throw new DatabaseException(e);
			}
		}

		mTableMetadata.getMarshaller().unmarshalValues(buffer, aOutput);
	}


	byte[] getKeys(Object aInput)
	{
		return mTableMetadata.getMarshaller().marshalKeys(new ByteArrayBuffer(16), aInput).trim().array();
	}


	byte[] getNonKeys(Object aInput)
	{
		return mTableMetadata.getMarshaller().marshalValues(new ByteArrayBuffer(16), aInput).trim().array();
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


	void scan()
	{
		mTableImplementation.scan();
	}


	public class TypeResult
	{
		public byte type;
	}
}

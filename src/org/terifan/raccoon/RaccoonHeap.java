package org.terifan.raccoon;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.TreeSet;
import java.util.function.Consumer;
import org.terifan.raccoon.blockdevice.BlockAccessor;
import org.terifan.raccoon.blockdevice.LobByteChannel;
import org.terifan.raccoon.blockdevice.LobOpenOption;
import org.terifan.raccoon.document.Document;


public class RaccoonHeap implements AutoCloseable
{
	private final static int RECENT_FREE_ENTRIES = 100;
	private final static int FREE = 0;
	private final static int INLINE = 1;
	private final static int EXTERNAL = 2;

	private Consumer<RaccoonHeap> mCloseAction;
	private LobByteChannel mChannel;
	private BlockAccessor mBlockAccessor;
	private TreeSet<Long> mFreeEntries;
	private int mRecordSize;


	RaccoonHeap(BlockAccessor aBlockAccessor, LobByteChannel aChannel, int aRecordSize, Consumer<RaccoonHeap> aCloseAction)
	{
		mBlockAccessor = aBlockAccessor;
		mChannel = aChannel;
		mRecordSize = aRecordSize;
		mCloseAction = aCloseAction;
		mFreeEntries = new TreeSet<>();
	}


	@Override
	public synchronized void close() throws IOException
	{
		if (mCloseAction != null)
		{
			mCloseAction.accept(this);
			mCloseAction = null;
		}
		if (mChannel != null)
		{
			mChannel.close();
			mChannel = null;
		}
	}


	public synchronized long save(Document aDocument) throws IOException
	{
		checkOpen();

		long index = mFreeEntries.isEmpty() ? mChannel.size() / mRecordSize : mFreeEntries.removeFirst();

		put(index, aDocument);

		return index;
	}


	public synchronized RaccoonHeap put(long aId, Document aDocument) throws IOException
	{
		checkOpen();

		putImpl(aId, aDocument);

		return this;
	}


	private void putImpl(long aId, Document aDocument) throws IOException
	{
		byte[] buf = aDocument.toByteArray();
		byte type = (byte)INLINE;

		if (buf.length > mRecordSize - 1)
		{
			Document header = new Document();
			try (LobByteChannel lob = new LobByteChannel(mBlockAccessor, header, LobOpenOption.WRITE, null, 1, false))
			{
				lob.writeAllBytes(aDocument.toByteArray());
			}
			buf = header.toByteArray();
			type = (byte)EXTERNAL;
		}

		ByteBuffer data = ByteBuffer.allocateDirect(mRecordSize);
		data.put(type);
		data.put(buf);
		data.position(0);

		mChannel.position(aId * mRecordSize);
		mChannel.write(data);
	}


	public synchronized Document get(long aId) throws IOException
	{
		checkOpen();

		if (aId > size())
		{
			throw new DatabaseException("Not a valid entry: " + aId);
		}

		ByteBuffer buf = ByteBuffer.allocate(mRecordSize);
		mChannel.position(aId * mRecordSize);
		mChannel.read(buf);

		switch (buf.get(0))
		{
			case FREE:
				throw new DatabaseException("Not a valid entry: " + aId);
			case INLINE:
				return new Document().fromByteArray(buf);
			case EXTERNAL:
				Document header = new Document().fromByteArray(buf);
				try (LobByteChannel lob = new LobByteChannel(mBlockAccessor, header, LobOpenOption.READ, null))
				{
					return new Document().fromByteArray(lob.readAllBytes());
				}
		}

		throw new DatabaseException("Not a valid entry: " + aId);
	}


	public synchronized boolean tryGet(Document aDocument) throws IOException
	{
		checkOpen();

		if (!aDocument.containsKey("_id") || !(aDocument.get("_id") instanceof Number))
		{
			throw new DatabaseException("Expected numeric _id in the provided Document");
		}

		try
		{
			aDocument.putAll(get(aDocument.getLong("_id")));

			return true;
		}
		catch (DatabaseException e)
		{
			return false;
		}
	}


	public synchronized Document delete(long aId) throws IOException
	{
		checkOpen();

		ByteBuffer buf = ByteBuffer.allocate(mRecordSize);
		mChannel.position(aId * mRecordSize);
		mChannel.read(buf);

		Document doc = null;
		switch (buf.get(0))
		{
			case FREE:
				throw new DatabaseException("Not a valid entry: " + aId);
			case INLINE:
				doc = new Document().fromByteArray(buf);
				break;
			case EXTERNAL:
				Document header = new Document().fromByteArray(buf);
				try (LobByteChannel lob = new LobByteChannel(mBlockAccessor, header, LobOpenOption.READ, null))
				{
					doc = new Document().fromByteArray(lob.readAllBytes());
					lob.delete();
				}
				break;
		}

		mFreeEntries.add(aId);
		if (mFreeEntries.size() > RECENT_FREE_ENTRIES)
		{
			mFreeEntries.removeLast();
		}

		return doc;
	}


	public synchronized boolean exists(long aId) throws IOException
	{
		checkOpen();

		if (size() <= aId)
		{
			return false;
		}

		byte[] buf = new byte[mRecordSize];
		mChannel.position(aId * mRecordSize);
		mChannel.readAllBytes(buf);

		mChannel.position(aId * mRecordSize);
		mChannel.writeAllBytes(new byte[mRecordSize]);

		return buf[0] != FREE;
	}


	public void checkOpen() throws IllegalStateException
	{
		if (mChannel == null)
		{
			throw new DatabaseException("This heap is closed.");
		}
	}


	public synchronized long size() throws IOException
	{
		return mChannel.size() / mRecordSize;
	}


//	public synchronized List<Document> query(Document aQuery)
//	{
//	}
//
//
//	public synchronizedvoid void query(Document aQuery, Consumer<Document> aConsumer)
//	{
//	}
//
//
//	public synchronizedvoid forEach(Consumer<Document> aConsumer)
//	{
//	}
}

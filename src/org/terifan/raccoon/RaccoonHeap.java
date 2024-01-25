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
	private final static byte FREE = 0;
	private final static byte INLINE = 1;
	private final static byte EXTERNAL = 2;

	private Consumer<RaccoonHeap> mCloseAction;
	private BlockAccessor mBlockAccessor;
	private TreeSet<Long> mFreeEntries;
	private LobByteChannel mChannel;
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


	/**
	 * Saves a record. If an _id value exists in the document that is used. Otherwise an _id will be computed and added to the document.
	 *
	 * @return the _id value of the saved document.
	 */
	public synchronized long save(Document aDocument) throws IOException
	{
		checkOpen();

		long id;
		if (aDocument.containsKey("_id"))
		{
			id = aDocument.getLong("_id");
		}
		else
		{
			id = mFreeEntries.isEmpty() ? mChannel.size() / mRecordSize : mFreeEntries.removeFirst();

			aDocument.put("_id", id);
		}

		putImpl(id, aDocument);

		return id;
	}


	public synchronized RaccoonHeap put(long aId, Document aDocument) throws IOException
	{
		checkOpen();

		putImpl(aId, aDocument);

		return this;
	}


	private void putImpl(long aId, Document aDocument) throws IOException
	{
		if (mBlockAccessor.getBlockDevice().isReadOnly())
		{
			throw new DatabaseException("This instance is Readonly.");
		}

		byte[] doc = aDocument.toByteArray();
		byte type = INLINE;

		if (doc.length >= mRecordSize)
		{
			Document header = new Document();
			try (LobByteChannel lob = new LobByteChannel(mBlockAccessor, header, LobOpenOption.WRITE))
			{
				lob.writeAllBytes(aDocument.toByteArray());
			}
			doc = header.toByteArray();
			type = EXTERNAL;
		}

		ByteBuffer data = ByteBuffer.allocate(mRecordSize);
		data.put(type);
		data.putShort((short)doc.length);
		data.put(doc);

		mChannel.position(aId * mRecordSize);
		mChannel.write(data.position(0));
	}


	/**
	 * Returns a record or throws an exception if not found.
	 */
	public synchronized Document get(long aId) throws IOException
	{
		Document doc = tryGet(aId);

		if (doc == null)
		{
			throw new DatabaseException("Entry not found: " + aId);
		}

		return doc;
	}


	/**
	 * Returns a record or null if not found.
	 */
	public synchronized Document tryGet(long aId) throws IOException
	{
		checkOpen();

		if (aId < 0)
		{
			throw new IllegalArgumentException("Negative ID not allowed: " + aId);
		}
		if (aId >= size())
		{
			return null;
		}

		ByteBuffer buf = ByteBuffer.allocate(mRecordSize);
		mChannel.position(aId * mRecordSize);
		mChannel.read(buf);

		if (buf.get(0) == FREE)
		{
			throw new DatabaseException("Not a valid entry: " + aId);
		}

		Document doc = new Document().fromByteArray(buf.position(1).limit(3 + buf.getShort()));

		if (buf.get(0) == EXTERNAL)
		{
			try (LobByteChannel lob = new LobByteChannel(mBlockAccessor, doc, LobOpenOption.READ))
			{
				doc = new Document().fromByteArray(lob.readAllBytes());
			}
		}

		return doc;
	}


	public synchronized boolean tryGet(Document aDocument) throws IOException
	{
		checkOpen();

		if (!aDocument.containsKey("_id"))
		{
			throw new DatabaseException("Expected numeric _id in the provided Document");
		}

		try
		{
			Long id = aDocument.getLong("_id");

			aDocument.putAll(get(id));

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

		if (mBlockAccessor.isReadOnly())
		{
			throw new DatabaseException("Cannot delete an entry from a Readonly storage.");
		}

		ByteBuffer buf = ByteBuffer.allocate(mRecordSize);
		mChannel.position(aId * mRecordSize);
		mChannel.read(buf);

		if (buf.get(0) == FREE)
		{
			return null;
		}

		Document doc = new Document().fromByteArray(buf.position(1).limit(3 + buf.getShort()));

		if (buf.get(0) == EXTERNAL)
		{
			try (LobByteChannel lob = new LobByteChannel(mBlockAccessor, doc, LobOpenOption.READ))
			{
				doc = new Document().fromByteArray(lob.readAllBytes());
				lob.delete();
			}
		}

		mChannel.position(aId * mRecordSize);
		mChannel.write(buf.position(0).put(FREE));

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


	LobByteChannel getChannel()
	{
		return mChannel;
	}
}

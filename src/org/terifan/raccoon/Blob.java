package org.terifan.raccoon;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import org.terifan.raccoon.storage.BlockAccessor;
import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;
import org.terifan.raccoon.util.Listener;


public class Blob implements SeekableByteChannel
{
	private final static boolean LOG = false;

	private final static int CHUNK_SIZE = 1024 * 1024;
	private final static int HEADER_SIZE = 8;
	private final static int INDIRECT_POINTER_THRESHOLD = 4;

	private BlockAccessor mBlockAccessor;
	private TransactionGroup mTransactionId;
	private ByteArrayBuffer mPersistedPointerBuffer;
	private ByteArrayBuffer mPersistedIndirectPointerBuffer;
	private HashMap<Integer,BlockPointer> mPendingBlockPointsers;
	private HashSet<Integer> mEmptyBlocks;
	private boolean mClosed;
	private Listener<Blob> mCloseListener;

	private long mTotalSize;
	private long mPosition;
	private byte[] mBuffer;
	private boolean mModified;
	private boolean mChunkModified;
	private int mChunkIndex;
	private BlockPointer mBlockPointer;
	private byte[] mHeader;


	Blob(BlockAccessor aBlockAccessor, TransactionGroup aTransactionId, byte[] aHeader, BlobOpenOption aOpenOption) throws IOException
	{
		mBlockAccessor = aBlockAccessor;
		mTransactionId = aTransactionId;
		mHeader = aHeader;

		mBuffer = new byte[CHUNK_SIZE];
		mPendingBlockPointsers = new HashMap<>();
		mEmptyBlocks = new HashSet<>();

		if (aHeader != null)
		{
			mPersistedPointerBuffer = ByteArrayBuffer.wrap(aHeader);
			mTotalSize = mPersistedPointerBuffer.readInt64();

			BlockPointer bp = new BlockPointer();
			bp.unmarshal(mPersistedPointerBuffer);

			if (bp.getBlockType() == BlockType.BLOB_INDEX)
			{
				mPersistedIndirectPointerBuffer = mPersistedPointerBuffer;
				mPersistedPointerBuffer = ByteArrayBuffer.wrap(mBlockAccessor.readBlock(bp)).limit(bp.getLogicalSize());
			}
		}

		if (aOpenOption == BlobOpenOption.APPEND)
		{
			mPosition = mTotalSize;
		}
		if (aOpenOption == BlobOpenOption.APPEND || aOpenOption == BlobOpenOption.WRITE)
		{
			mChunkIndex = -1; // force sync to load the block at mPosition
		}
	}


	@Override
	public int read(ByteBuffer aDst) throws IOException
	{
		if (mClosed)
		{
			throw new ClosedChannelException();
		}

		int limit = aDst.limit() - aDst.position();

		int total = (int)Math.min(limit, mTotalSize - mPosition);

		if (total == 0)
		{
			return -1;
		}

		if (LOG) System.out.println("READ  " + mPosition + " +" + total);

		int posInChunk = (int)(mPosition % CHUNK_SIZE);

		for (int remaining = total; remaining > 0; )
		{
			sync(false);

			int length = Math.min(remaining, CHUNK_SIZE - posInChunk);

			if (LOG) System.out.println("\tAppend " + posInChunk + " +" + length);

			aDst.put(mBuffer, posInChunk, length);
			mPosition += length;
			remaining -= length;

			posInChunk = 0;
		}

		return total;
	}


	@Override
	public int write(ByteBuffer aSrc) throws IOException
	{
		if (mClosed)
		{
			throw new ClosedChannelException();
		}

		int total = aSrc.limit() - aSrc.position();

		if (LOG) System.out.println("WRITE " + mPosition + " +" + total);

		int posInChunk = (int)(mPosition % CHUNK_SIZE);

		for (int remaining = total; remaining > 0;)
		{
			sync(false);

			int length = Math.min(remaining, CHUNK_SIZE - posInChunk);

			if (LOG) System.out.println("\tAppend " + posInChunk + " +" + length);

			aSrc.get(mBuffer, posInChunk, length);
			mPosition += length;
			remaining -= length;

			mTotalSize = Math.max(mTotalSize, mPosition);
			mChunkModified = true;
			posInChunk = 0;
		}

		mModified = true;

		return total;
	}


	@Override
	public long position() throws IOException
	{
		return mPosition;
	}


	@Override
	public SeekableByteChannel position(long aNewPosition) throws IOException
	{
		if (mClosed)
		{
			throw new ClosedChannelException();
		}

		mPosition = aNewPosition;
		return this;
	}


	@Override
	public long size() throws IOException
	{
		return mTotalSize;
	}


	@Override
	public SeekableByteChannel truncate(long aSize) throws IOException
	{
		if (mClosed)
		{
			throw new ClosedChannelException();
		}

		throw new UnsupportedOperationException();

//		if (aSize < mSize)
//		{
//			sync(true);
//
//			for (long partIndex = aSize / Blob.CHUNK_SIZE; partIndex < mSize / Blob.CHUNK_SIZE; partIndex++)
//			{
//				mFile.mFileSystem.getDatabase().remove(new BlobPtr(mFile.getObjectId(), partIndex));
//			}
//
//			if ((aSize % CHUNK_SIZE) > 0)
//			{
//				long ci = aSize / CHUNK_SIZE;
//
//				byte[] tmp = Streams.readAll(mFile.mFileSystem.getDatabase().load(new BlobPtr(mFile.getObjectId(), ci)));
//
//				Arrays.fill(tmp, CHUNK_SIZE - (int)(aSize % CHUNK_SIZE), CHUNK_SIZE, (byte)0);
//
//				mFile.mFileSystem.getDatabase().save(new BlobPtr(mFile.getObjectId(), ci), new ByteArrayInputStream(mBuffer, 0, (int)(mSize % CHUNK_SIZE)));
//			}
//
//			mSize = aSize;
//
//			if (mPosition > mSize)
//			{
//				mPosition = mSize;
//			}
//		}
//		return this;
	}


	@Override
	public boolean isOpen()
	{
		return !mClosed;
	}


	synchronized byte[] finish() throws IOException
	{
		if (mClosed || !mModified)
		{
			return mHeader;
		}

		sync(true);

		Log.d("closing blob");
		Log.inc();

		int pointerCount = (int)((mTotalSize + CHUNK_SIZE - 1) / CHUNK_SIZE);
		ByteArrayBuffer buf = ByteArrayBuffer.alloc(HEADER_SIZE + BlockPointer.SIZE * pointerCount, true);
		buf.limit(buf.capacity());

		if (LOG) System.out.println("CLOSE +" + mTotalSize);

		buf.writeInt64(mTotalSize);

		for (int i = 0; i < pointerCount; i++)
		{
			if (mEmptyBlocks.contains(i))
			{
				if (LOG) System.out.println("\tempty");
				buf.write(new byte[BlockPointer.SIZE]);
			}
			else
			{
				BlockPointer bp = mPendingBlockPointsers.get(i);
				if (bp != null)
				{
					bp.marshal(buf);
					if (LOG) System.out.println("\tnew " + bp);
				}
				else if (mPersistedPointerBuffer != null && mPersistedPointerBuffer.capacity() > buf.position())
				{
					mPersistedPointerBuffer.position(HEADER_SIZE + i * BlockPointer.SIZE).copyTo(buf, BlockPointer.SIZE);
					if (LOG) System.out.println("\told " + new BlockPointer().unmarshal(ByteArrayBuffer.wrap(mPersistedPointerBuffer.array()).position(HEADER_SIZE + i * BlockPointer.SIZE)));
				}
			}
		}

		if (pointerCount > INDIRECT_POINTER_THRESHOLD)
		{
			Log.d("created indirect blob pointer block");
			Log.inc();

			buf.position(HEADER_SIZE);
			BlockPointer bp = mBlockAccessor.writeBlock(buf.array(), 0, buf.capacity(), mTransactionId.get(), BlockType.BLOB_INDEX, 0);
			bp.marshal(buf);
			buf.trim();

			Log.dec();
		}

		if (mPersistedIndirectPointerBuffer != null)
		{
			Log.d("freed indirect block");
			Log.inc();

			BlockPointer bp = new BlockPointer();
			bp.unmarshal(mPersistedIndirectPointerBuffer.position(HEADER_SIZE));
			mBlockAccessor.freeBlock(bp);

			Log.dec();
		}

		mHeader = buf.array();
		mClosed = true;

		Log.dec();

		return mHeader;
	}


	@Override
	public void close() throws IOException
	{
		if (mCloseListener != null)
		{
			mCloseListener.call(this);
		}
	}


	private void sync(boolean aFinal) throws IOException
	{
		if (LOG) System.out.println("\tsync pos: " + mPosition + ", size: " + mTotalSize + ", final: " + aFinal + ", posChunk: " + (mPosition % CHUNK_SIZE) + ", indexChunk:" + mPosition / CHUNK_SIZE + ", mod: " + mChunkModified);

		if ((mPosition % CHUNK_SIZE) == 0 || mPosition / CHUNK_SIZE != mChunkIndex || aFinal)
		{
			if (mChunkModified)
			{
				int len = (int)Math.min(CHUNK_SIZE, mTotalSize - CHUNK_SIZE * mChunkIndex);

				if (LOG) System.out.println("\tWriting chunk " + mChunkIndex + " " + len + " bytes");

				BlockPointer old = mPendingBlockPointsers.remove(mChunkIndex);
				if (old != null)
				{
					mBlockAccessor.freeBlock(old);
					mEmptyBlocks.remove(mChunkIndex);
				}

				if (isAllZeros())
				{
					mEmptyBlocks.add(mChunkIndex);
				}
				else
				{
					BlockPointer bp = mBlockAccessor.writeBlock(mBuffer, 0, len, mTransactionId.get(), BlockType.BLOB_DATA, 0);
					mPendingBlockPointsers.put(mChunkIndex, bp);
				}
			}

			if (!aFinal)
			{
				Arrays.fill(mBuffer, (byte)0);

				mChunkIndex = (int)(mPosition / CHUNK_SIZE);
				mChunkModified = false;

				mBlockPointer = mPendingBlockPointsers.get(mChunkIndex);

				if (mBlockPointer == null && !mEmptyBlocks.contains(mChunkIndex))
				{
					int o = HEADER_SIZE + mChunkIndex * BlockPointer.SIZE;

					if (mPersistedPointerBuffer != null && mPersistedPointerBuffer.capacity() > o)
					{
						mBlockPointer = new BlockPointer();
						mBlockPointer.unmarshal(mPersistedPointerBuffer.position(o));
					}
				}

				if (mBlockPointer != null)
				{
					if (LOG) System.out.println("\tReading chunk " + mChunkIndex);

					byte[] tmp = mBlockAccessor.readBlock(mBlockPointer);
					System.arraycopy(tmp, 0, mBuffer, 0, tmp.length);
				}
			}
		}
	}


	private boolean isAllZeros()
	{
		for (int i = 0; i < mBuffer.length; i++)
		{
			if (mBuffer[i] != 0)
			{
				return false;
			}
		}
		return true;
	}


	static void deleteBlob(BlockAccessor aBlockAccessor, byte[] aHeader)
	{
		freeBlocks(aBlockAccessor, ByteArrayBuffer.wrap(aHeader).position(HEADER_SIZE));
	}


	private static void freeBlocks(BlockAccessor aBlockAccessor, ByteArrayBuffer aBuffer)
	{
		while (aBuffer.remaining() > 0)
		{
			BlockPointer bp = new BlockPointer().unmarshal(aBuffer);

			if (bp.getBlockType() == BlockType.BLOB_INDEX)
			{
				freeBlocks(aBlockAccessor, ByteArrayBuffer.wrap(aBlockAccessor.readBlock(bp)).limit(bp.getLogicalSize()).position(HEADER_SIZE));
			}

			aBlockAccessor.freeBlock(bp);
		}
	}


	public byte[] readAllBytes() throws IOException
	{
		ByteBuffer buf = ByteBuffer.allocate((int)size());
		read(buf);
		return buf.array();
	}


	public Blob readAllBytes(OutputStream aDst) throws IOException
	{
		ByteBuffer buf = ByteBuffer.allocate(1024);

		for (long remaining = size(); remaining > 0;)
		{
			int len = read(buf);
			if (len == -1)
			{
				throw new IOException("Unexpected end of stream");
			}
			aDst.write(buf.array(), 0, len);
		}

		return this;
	}


	public Blob writeAllBytes(byte[] aSrc) throws IOException
	{
		write(ByteBuffer.wrap(aSrc));
		return this;
	}


	public Blob writeAllBytes(InputStream aSrc) throws IOException
	{
		ByteBuffer buf = ByteBuffer.allocate(1024);

		for (int len; (len = aSrc.read(buf.array())) > 0;)
		{
			buf.position(0).limit(len);
			write(buf);
		}

		return this;
	}


	public InputStream newInputStream()
	{
		return new InputStream()
		{
			long streamPosition = mPosition;
			ByteBuffer buffer = (ByteBuffer)ByteBuffer.allocate(4096).position(4096);


			@Override
			public int read() throws IOException
			{
				if (buffer.position() == buffer.limit())
				{
					if (streamPosition == size())
					{
						return -1;
					}

					position(streamPosition);
					buffer.position(0);
					Blob.this.read(buffer);
					buffer.flip();
					streamPosition = position();
				}

				return 0xff & buffer.get();
			}


			@Override
			public int read(byte[] aBuffer, int aOffset, int aLength) throws IOException
			{
				int total = 0;

				for (int remaining = aLength; remaining > 0;)
				{
					if (buffer.position() == buffer.limit())
					{
						if (streamPosition == size())
						{
							break;
						}

						position(streamPosition);
						buffer.position(0);
						Blob.this.read(buffer);
						buffer.flip();
						streamPosition = position();
					}

					int len = Math.min(remaining, buffer.remaining());

					buffer.get(aBuffer, aOffset + total, len);
					remaining -= len;
					total += len;
				}

				return total == 0 ? -1 : total;
			}


			@Override
			public void close() throws IOException
			{
				Blob.this.close();
			}
		};
	}


	public OutputStream newOutputStream()
	{
		return new OutputStream()
		{
			ByteBuffer buffer = (ByteBuffer)ByteBuffer.allocate(4096);


			@Override
			public void write(int aByte) throws IOException
			{
				buffer.put((byte)aByte);

				if (buffer.position() == buffer.capacity())
				{
					buffer.flip();
					Blob.this.write(buffer);
					buffer.clear();
				}
			}


			@Override
			public void write(byte[] aBuffer, int aOffset, int aLength) throws IOException
			{
				for (int remaining = aLength; remaining > 0;)
				{
					int len = Math.min(remaining, buffer.remaining());

					buffer.put(aBuffer, aOffset, len);

					remaining -= len;
					aOffset += len;

					if (buffer.position() == buffer.capacity())
					{
						buffer.flip();
						Blob.this.write(buffer);
						buffer.clear();
					}
				}
			}


			@Override
			public void close() throws IOException
			{
				if (buffer.position() > 0)
				{
					buffer.flip();
					Blob.this.write(buffer);
				}

				Blob.this.close();
			}
		};
	}


	public boolean isModified()
	{
		return mModified;
	}


	public Listener<Blob> getCloseListener()
	{
		return mCloseListener;
	}


	public Blob setCloseListener(Listener<Blob> aListener)
	{
		mCloseListener = aListener;
		return this;
	}
}

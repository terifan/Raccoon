package org.terifan.raccoon;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import org.terifan.raccoon.storage.BlockAccessor;
import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;


public class Blob implements SeekableByteChannel
{
	private final static boolean LOG = false;

	final static int CHUNK_SIZE = 1024;
	private static final int HEADER_SIZE = 8;
	private final static int INDIRECT_POINTER_THRESHOLD = 4;

	private BlockAccessor mBlockAccessor;
	private TransactionGroup mTransactionId;
	private ByteArrayBuffer mOriginalPointerBuffer;
	private ByteArrayBuffer mOldIndirectPointerBuffer;
	private HashMap<Integer,BlockPointer> mBlockPointsers;
	private HashSet<Integer> mEmptyBlocks;
	private boolean mClosed;
	private byte[] mHeader;

	private long mTotalSize;
	private long mPosition;
	private byte[] mBuffer;
	private boolean mModified;
	private boolean mChunkModified;
	private int mChunkIndex;
	private BlockPointer mBlockPointer;


	Blob(BlockAccessor aBlockAccessor, TransactionGroup aTransactionId, ByteArrayBuffer aPointerBuffer, BlobOpenOption aOpenOption) throws IOException
	{
		mBlockAccessor = aBlockAccessor;
		mTransactionId = aTransactionId;
		mOriginalPointerBuffer = aPointerBuffer;

		mBuffer = new byte[CHUNK_SIZE];
		mBlockPointsers = new HashMap<>();
		mEmptyBlocks = new HashSet<>();

		if (mOriginalPointerBuffer.capacity() > 0)
		{
			mTotalSize = mOriginalPointerBuffer.readInt64();

			BlockPointer bp = new BlockPointer();
			bp.unmarshal(mOriginalPointerBuffer);

			if (bp.getBlockType() == BlockType.BLOB_INDEX)
			{
				mOldIndirectPointerBuffer = mOriginalPointerBuffer;
				mOriginalPointerBuffer = new ByteArrayBuffer(mBlockAccessor.readBlock(bp));
			}
		}

		if (aOpenOption == BlobOpenOption.APPEND)
		{
			mPosition = mTotalSize;
		}
	}


	@Override
	public int read(ByteBuffer aDst) throws IOException
	{
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
		mPosition = (int)aNewPosition;
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
		return this;
	}


	@Override
	public boolean isOpen()
	{
		return !mClosed;
	}


	@Override
	public synchronized void close() throws IOException
	{
		if (mModified)
		{
			sync(true);

			Log.d("closing blob");
			Log.inc();

			int pointerCount = (int)((mTotalSize + CHUNK_SIZE - 1) / CHUNK_SIZE);
			ByteArrayBuffer buf = new ByteArrayBuffer(HEADER_SIZE + BlockPointer.SIZE * pointerCount);
			buf.limit(buf.capacity());

			buf.writeInt64(mTotalSize);

			for (int i = 0; i < pointerCount; i++)
			{
				if (mEmptyBlocks.contains(i))
				{
					buf.write(new byte[BlockPointer.SIZE]);
				}
				else
				{
					BlockPointer bp = mBlockPointsers.get(i);
					if (bp != null)
					{
						bp.marshal(buf);
					}
					else if (mOriginalPointerBuffer.capacity() > buf.position())
					{
						mOriginalPointerBuffer.position(HEADER_SIZE + i * BlockPointer.SIZE).copyTo(buf, BlockPointer.SIZE);
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

			if (mOldIndirectPointerBuffer != null)
			{
				Log.d("freed indirect block");
				Log.inc();

				BlockPointer bp = new BlockPointer();
				bp.unmarshal(mOldIndirectPointerBuffer.position(HEADER_SIZE));
				mBlockAccessor.freeBlock(bp);

				Log.dec();
			}

			mHeader = buf.array();

			mModified = false;

			onClose();

			Log.dec();
		}

		mClosed = true;
	}


	private void sync(boolean aFinal) throws IOException
	{
		if (LOG) System.out.println("\tsync pos: " + mPosition + ", size: " + mTotalSize + ", final: " + aFinal + ", posChunk: " + (mPosition % CHUNK_SIZE) + ", indexChunk:" + mPosition / CHUNK_SIZE);

		if ((mPosition % CHUNK_SIZE) == 0 || mPosition / CHUNK_SIZE != mChunkIndex || aFinal)
		{
			if (mChunkModified)
			{
				int len = (int)Math.min(CHUNK_SIZE, mTotalSize - CHUNK_SIZE * mChunkIndex);

				if (LOG) System.out.println("\tWriting chunk " + mChunkIndex + " " + len + " bytes");

				BlockPointer old = mBlockPointsers.remove(mChunkIndex);
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
					mBlockPointsers.put(mChunkIndex, bp);
				}
			}

			if (!aFinal)
			{
				Arrays.fill(mBuffer, (byte)0);

				mChunkIndex = (int)(mPosition / CHUNK_SIZE);
				mChunkModified = false;

				mBlockPointer = mBlockPointsers.get(mChunkIndex);

				if (mBlockPointer == null && !mEmptyBlocks.contains(mChunkIndex))
				{
					int o = HEADER_SIZE + mChunkIndex * BlockPointer.SIZE;

					if (mOriginalPointerBuffer.capacity() > o)
					{
						mBlockPointer = new BlockPointer();
						mBlockPointer.unmarshal(mOriginalPointerBuffer.position(o));
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


	byte[] getHeader()
	{
		return mHeader;
	}


	void onClose()
	{
	}


	void onException(Exception aException)
	{
	}
}

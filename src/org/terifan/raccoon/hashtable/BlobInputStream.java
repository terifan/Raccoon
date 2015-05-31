package org.terifan.raccoon.hashtable;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.terifan.raccoon.util.ByteArray;
import org.terifan.raccoon.DatabaseException;
import static org.terifan.raccoon.hashtable.Blob.HEADER_FIELD_BLOCK_KEY;
import org.terifan.raccoon.io.IManagedBlockDevice;


class BlobInputStream extends InputStream
{
	private final static String TAG = BlobInputStream.class.getName();

	private IManagedBlockDevice mBlockDevice;
	private DataInputStream mFragmentPointers;
	private byte[] mBuffer;
	private int mPageSize;
	private int mOffset;
	private int mRemaining;
	private int mFragmentCount;
	private int mFragmentIndex;
	private long mBlockKey;
	private long mTransactionId;


	public BlobInputStream(IManagedBlockDevice aBlockDevice, byte[] aBlobInfo)
	{
		try
		{
			mBlockDevice = aBlockDevice;
			mPageSize = mBlockDevice.getBlockSize();
			mBuffer = new byte[0];

			mRemaining = ByteArray.getInt(aBlobInfo, Blob.HEADER_FIELD_LENGTH);
			mFragmentCount = ByteArray.getInt(aBlobInfo, Blob.HEADER_FIELD_COUNT);
			mTransactionId = ByteArray.getUnsignedInt(aBlobInfo, Blob.HEADER_FIELD_TRANSACTION);
			mBlockKey = ByteArray.getUnsignedInt(aBlobInfo, HEADER_FIELD_BLOCK_KEY);

			mFragmentPointers = new DataInputStream(new ByteArrayInputStream(aBlobInfo));
			mFragmentPointers.skip(Blob.HEADER_SIZE);

	//		mHashTable.d("Read blob, fragments: " + mFragmentCount + ", bytes: " + mRemaining);

			// read indirect block
			if (mFragmentCount < 0)
			{
				int blockIndex = (int)ByteArray.readVarLong(mFragmentPointers);
				int blockCount = (int)ByteArray.readVarLong(mFragmentPointers) + 1;

	//			mHashTable.d("Read indirect block at " + blockIndex + " +" + blockCount);

				byte[] indirectBuffer = new byte[mPageSize * blockCount];
				mBlockDevice.readBlock(blockIndex, indirectBuffer, 0, indirectBuffer.length, (mTransactionId << 32) | mBlockKey);

				mFragmentPointers = new DataInputStream(new ByteArrayInputStream(indirectBuffer));
				mFragmentCount = Math.abs(mFragmentCount);
			}
		}
		catch (IOException e)
		{
			throw new DatabaseException(e);
		}
	}


	@Override
	public int read() throws IOException
	{
		if (mRemaining <= 0)
		{
			return -1;
		}
		if (mOffset == mBuffer.length)
		{
			load();
		}

		mRemaining--;
		return 0xff & mBuffer[mOffset++];
	}


	@Override
	public int read(byte[] aBuffer, int aOffset, int aLength) throws IOException
	{
		int readCount = 0;

		while (readCount < aLength && mRemaining > 0)
		{
			if (mOffset == mBuffer.length)
			{
				load();
			}

			int len = Math.min(Math.min(aLength - readCount, mBuffer.length - mOffset), mRemaining);

			System.arraycopy(mBuffer, mOffset, aBuffer, aOffset + readCount, len);

			mOffset += len;
			mRemaining -= len;
			readCount += len;
		}

		return readCount == 0 ? mRemaining <= 0 ? -1 : 0 : readCount;
	}


	private void load() throws IOException
	{
		if (mFragmentIndex++ >= mFragmentCount)
		{
			throw new IOException();
		}

		int blockIndex = (int)ByteArray.readVarLong(mFragmentPointers);
		int blockCount = (int)ByteArray.readVarLong(mFragmentPointers) + 1;
		int len = mPageSize * blockCount;

		if (mBuffer.length != len)
		{
			mBuffer = new byte[len];
		}

//		mHashTable.d("Read fragment " + mFragmentIndex + " at " + blockIndex + " +" + blockCount);

		mBlockDevice.readBlock(blockIndex, mBuffer, 0, len, (mTransactionId << 32) | mBlockKey);

		mOffset = 0;
	}
}

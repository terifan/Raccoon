package org.terifan.raccoon.io;

import java.io.IOException;
import java.io.InputStream;
import org.terifan.raccoon.util.ByteArray;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;

// #
// xxxx
// ....... ....... ........ ........

public class BlobInputStream extends InputStream implements AutoCloseable
{
	private final static String TAG = BlobInputStream.class.getName();

	private IManagedBlockDevice mBlockDevice;
	private ByteArrayBuffer mPointerBuffer;
	private ByteArrayBuffer mBuffer;
	private int mPageSize;
	private long mRemaining;
	private long mFragmentIndex;
	private long mTransactionId;
	private boolean mIndirect;

	private BlobInputStream mSubStream;


	public BlobInputStream(IManagedBlockDevice aBlockDevice, byte[] aHeader) throws IOException
	{
		mBlockDevice = aBlockDevice;
		mPageSize = mBlockDevice.getBlockSize();
		mBuffer = new ByteArrayBuffer(0);

		mPointerBuffer = new ByteArrayBuffer(aHeader);
		mIndirect = mPointerBuffer.readBit() == 1;
		mRemaining = mPointerBuffer.readVar64();
		mTransactionId = mPointerBuffer.readVar64();
		int pointerBufferLength = mPointerBuffer.readVar32();

		if (mIndirect)
		{
			long blockIndex = mPointerBuffer.readVar64();
			int blockCount = mPointerBuffer.readVar32() + 1;
			long blockKey = mPointerBuffer.readInt64();

			mPointerBuffer = new ByteArrayBuffer(mPageSize * blockCount);

			mBlockDevice.readBlock(blockIndex, mPointerBuffer.array(), 0, mPointerBuffer.capacity(), blockKey);
		}
	}


	@Override
	public int read() throws IOException
	{
		if (mRemaining <= 0)
		{
			return -1;
		}

		if (mBuffer.remaining() == 0)
		{
			loadBlock();
		}

		mRemaining--;
		return mBuffer.read();
	}


	@Override
	public int read(byte[] aBuffer, int aOffset, int aLength) throws IOException
	{
		int readCount = 0;

		while (readCount < aLength && mRemaining > 0)
		{
			if (mBuffer.remaining() == 0)
			{
				loadBlock();
			}

			int len = mBuffer.read(aBuffer, aOffset, (int)Math.min(aLength - readCount, mRemaining));

			mRemaining -= len;
			readCount += len;
		}

		return readCount == 0 ? mRemaining <= 0 ? -1 : 0 : readCount;
	}


	@Override
	public void close() throws IOException
	{
	}


	private void loadBlock() throws IOException
	{
		if (mPointerBuffer.remaining() == 0)
		{
			throw new IOException();
		}

		long blockIndex = mPointerBuffer.readVar64();
		int blockCount = mPointerBuffer.readVar32() + 1;
		long blockKey = mPointerBuffer.readInt64();
		int len = mPageSize * blockCount;

		Log.d("Read fragment " + ++mFragmentIndex + " at " + blockIndex + " +" + blockCount);

		mBuffer.capacity(len);
		mBuffer.position(0);

		mBlockDevice.readBlock(blockIndex, mBuffer.array(), 0, len, blockKey);
	}
}

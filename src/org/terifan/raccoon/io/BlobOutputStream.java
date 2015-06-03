package org.terifan.raccoon.io;

import java.io.IOException;
import java.io.OutputStream;
import org.terifan.raccoon.security.ISAAC;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;


public class BlobOutputStream extends OutputStream
{
	final static int MAX_ADJACENT_BLOCKS = 64;
	final static int POINTER_MAX_LENGTH = 64;

	private int mBlockSize;
	private ByteArrayBuffer mBuffer;
	private ByteArrayBuffer mPointerBuffer;
	private IManagedBlockDevice mBlockDevice;
	private long mTransactionId;
	private long mTotalLength;
	private long mFragmentCounter;
	private byte[] mHeader;
	private boolean mClosed;


	public BlobOutputStream(IManagedBlockDevice aBlockDevice, long aTransactionId) throws IOException
	{
		mBlockDevice = aBlockDevice;
		mTransactionId = aTransactionId;

		mBlockSize = mBlockDevice.getBlockSize();
		mBuffer = new ByteArrayBuffer(MAX_ADJACENT_BLOCKS * mBlockSize);
		mPointerBuffer = new ByteArrayBuffer(mBlockSize);
	}


	@Override
	public void write(int b) throws IOException
	{
		if (mClosed)
		{
			throw new IOException("Stream closed");
		}

		mBuffer.write(b);
		mTotalLength++;

		if (mBuffer.position() == mBuffer.capacity())
		{
			flushBlock();
		}
	}


	@Override
	public void close() throws IOException
	{
		if (!mClosed)
		{
			int padLength = mBlockSize - (mBuffer.position() % mBlockSize);
			if (padLength > 0 && padLength < mBlockSize)
			{
				mBuffer.write(new byte[padLength]);
			}

			flushBlock();

			closeStream();
		}
	}


	public byte[] getHeader() throws IOException
	{
		if (!mClosed)
		{
			throw new IOException("Stream not closed");
		}

		return mHeader;
	}


	private void flushBlock() throws IOException
	{
		assert mBuffer.position() % mBlockSize == 0 : mBuffer.position();

		int blockCount = mBuffer.position() / mBlockSize;
		long blockIndex = mBlockDevice.allocBlock(blockCount);
		long blockKey = ISAAC.PRNG.nextLong();
		if (blockIndex < 0)
		{
			throw new IOException("Insufficient space in block device.");
		}

		mBlockDevice.writeBlock(blockIndex, mBuffer.array(), 0, blockCount * mBlockSize, blockKey);

		mPointerBuffer.writeVar64(blockIndex);
		mPointerBuffer.writeVar64(blockCount);
		mPointerBuffer.writeInt64(blockKey);

		Log.d("Write fragment " + ++mFragmentCounter + " at " + blockIndex + " +" + blockCount);

		mBuffer.position(0);
	}


	private void closeStream() throws IOException
	{
		boolean indirectBlock = false;
		int pointerBufferLength = mPointerBuffer.position();

		// create indirect block if pointers exceed max length
		if (pointerBufferLength > POINTER_MAX_LENGTH)
		{
			BlobOutputStream bos = new BlobOutputStream(mBlockDevice, mTransactionId);
			bos.write(mPointerBuffer.array(), 0, pointerBufferLength);
			bos.close();

			byte[] header = bos.getHeader();
			indirectBlock = true;
			pointerBufferLength = header.length;
			mPointerBuffer.array(header).position(header.length);
		}

		ByteArrayBuffer output = new ByteArrayBuffer(pointerBufferLength);
		output.writeBit(indirectBlock);
		output.writeVar64(mTotalLength);
		output.writeVar64(pointerBufferLength);
		output.writeVar64(mTransactionId);
		output.write(mPointerBuffer.array(), 0, pointerBufferLength);

		Log.out.println(mTotalLength);

		mHeader = output.trim().array();
		mClosed = true;
	}
}
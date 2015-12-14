package org.terifan.raccoon.io;

import java.io.IOException;
import java.io.InputStream;
import org.terifan.raccoon.util.ByteArrayBuffer;
import static org.terifan.raccoon.io.BlockPointer.Types.*;


public class BlobInputStream extends InputStream implements AutoCloseable
{
	private final static String TAG = BlobInputStream.class.getName();

	private BlockAccessor mBlockAccessor;
	private ByteArrayBuffer mPointerBuffer;
	private ByteArrayBuffer mBuffer;
	private long mRemaining;


	public BlobInputStream(BlockAccessor aBlockAccessor, byte[] aPointerBuffer) throws IOException
	{
		this(aBlockAccessor, new ByteArrayBuffer(aPointerBuffer));
	}


	public BlobInputStream(BlockAccessor aBlockAccessor, ByteArrayBuffer aPointerBuffer) throws IOException
	{
		mBlockAccessor = aBlockAccessor;
		mPointerBuffer = aPointerBuffer;
		mRemaining = mPointerBuffer.readVar64();

		if (mRemaining > 0)
		{
			BlockPointer bp = loadBlock();

			if (bp.getType() == BLOB_INDIRECT)
			{
				mPointerBuffer = mBuffer;

				loadBlock();
			}
		}
	}


	@Override
	public int available() throws IOException
	{
		return (int)Math.min(Integer.MAX_VALUE, mRemaining);
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
		if (mRemaining <= 0)
		{
			return -1;
		}

		int bytesRead = 0;

		for (int remaining = (int)Math.min(aLength, mRemaining); remaining > 0;)
		{
			if (mBuffer.remaining() == 0)
			{
				loadBlock();
			}

			int len = mBuffer.read(aBuffer, aOffset, remaining);
			remaining -= len;
			bytesRead += len;
			aOffset += len;
		}

		mRemaining -= bytesRead;

		return bytesRead;
	}


	@Override
	public void close() throws IOException
	{
		mRemaining = 0;
		mBlockAccessor = null;
		mPointerBuffer = null;
		mBuffer = null;
	}


	private BlockPointer loadBlock() throws IOException
	{
		if (mPointerBuffer.remaining() == 0)
		{
			throw new IOException();
		}

		BlockPointer bp = new BlockPointer();
		bp.unmarshal(mPointerBuffer);
		mBuffer = new ByteArrayBuffer(mBlockAccessor.readBlock(bp));

		return bp;
	}
}

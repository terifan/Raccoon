package org.terifan.raccoon.io;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;


public class BlobOutputStream extends OutputStream implements AutoCloseable
{
	final static int TYPE_DATA = 0;
	final static int TYPE_INDIRECT = 1;
	final static int FRAGMENT_SIZE = 1024 * 1024;
	private final static int POINTER_MAX_LENGTH = 4 * BlockPointer.SIZE;

	private BlockAccessor mBlockAccessor;
	private ByteArrayBuffer mBuffer;
	private ByteArrayBuffer mPointerBuffer;
	private long mTransactionId;
	private long mTotalLength;
	private byte[] mHeader;
	private boolean mClosed;
	private OnCloseListener mOnCloseListener;


	public BlobOutputStream(BlockAccessor aBlockAccessor, long aTransactionId) throws IOException
	{
		mBlockAccessor = aBlockAccessor;
		mTransactionId = aTransactionId;
		mBuffer = new ByteArrayBuffer(FRAGMENT_SIZE);
		mPointerBuffer = new ByteArrayBuffer(mBlockAccessor.getBlockDevice().getBlockSize());
	}


	public void setOnCloseListener(OnCloseListener aOnCloseListener)
	{
		mOnCloseListener = aOnCloseListener;
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

		if (mBuffer.remaining() == 0)
		{
			flushBlock();
		}
	}


	@Override
	public void close() throws IOException
	{
		if (mClosed)
		{
			return;
		}

		Log.v("closing blob");
		Log.inc();

		if (mBuffer.position() > 0)
		{
			flushBlock();
		}

		int pointerBufferLength = mPointerBuffer.position();

		// create indirect block if pointers exceed max length
		if (pointerBufferLength > POINTER_MAX_LENGTH)
		{
			BlockPointer bp = mBlockAccessor.writeBlock(mPointerBuffer.array(), 0, mPointerBuffer.position(), mTransactionId, TYPE_INDIRECT, 0);
			bp.marshal(mPointerBuffer.position(0));
			pointerBufferLength = BlockPointer.SIZE;
		}

		ByteArrayBuffer output = new ByteArrayBuffer(pointerBufferLength);
		output.writeVar64(mTotalLength);
		output.write(mPointerBuffer.array(), 0, pointerBufferLength);

		mHeader = output.trim().array();
		mClosed = true;

		if (mOnCloseListener != null)
		{
			mOnCloseListener.onClose(mHeader);
		}

		Log.dec();
	}


	public byte[] finish() throws IOException
	{
		close();

		return mHeader;
	}


	private void flushBlock() throws IOException
	{
		BlockPointer bp = mBlockAccessor.writeBlock(mBuffer.array(), 0, mBuffer.position(), mTransactionId, TYPE_DATA, 0);
		bp.marshal(mPointerBuffer);
		mBuffer.position(0);

		Arrays.fill(mBuffer.array(), (byte)0);
	}
	
	
	public interface OnCloseListener
	{
		void onClose(byte[] aHeader);
	}
}
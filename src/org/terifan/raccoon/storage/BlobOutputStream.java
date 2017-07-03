package org.terifan.raccoon.storage;

import org.terifan.raccoon.core.BlockType;
import java.io.IOException;
import java.io.OutputStream;
import org.terifan.raccoon.TransactionGroup;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;


public class BlobOutputStream extends OutputStream
{
	private final static int FRAGMENT_SIZE = 1024 * 1024;
	private final static int POINTER_MAX_LENGTH = 4 * BlockPointer.SIZE;

	private final BlockAccessor mBlockAccessor;
	private final ByteArrayBuffer mBuffer;
	private final ByteArrayBuffer mPointerBuffer;
	private final TransactionGroup mTransactionId;
	private long mTotalLength;
	private byte[] mHeader;
	private boolean mClosed;
	private OnCloseListener mOnCloseListener;


	public BlobOutputStream(BlockAccessor aBlockAccessor, TransactionGroup aTransactionId, OnCloseListener aOnCloseListener) throws IOException
	{
		mBlockAccessor = aBlockAccessor;
		mTransactionId = aTransactionId;
		mBuffer = new ByteArrayBuffer(FRAGMENT_SIZE);
		mPointerBuffer = new ByteArrayBuffer(mBlockAccessor.getBlockDevice().getBlockSize());
		mOnCloseListener = aOnCloseListener;
		mHeader = null;
	}


	@Override
	public void write(int b) throws IOException
	{
		if (mClosed)
		{
			throw new IOException("Stream closed");
		}

		mBuffer.writeInt8(b);
		mTotalLength++;

		if (mBuffer.remaining() == 0)
		{
			flushBlock();
		}
	}


	@Override
	public void write(byte[] aBuffer, int aOffset, int aLength) throws IOException
	{
		if (mClosed)
		{
			throw new IOException("Stream closed");
		}

		while (aLength > 0)
		{
			int len = Math.min(mBuffer.remaining(), aLength);

			mBuffer.write(aBuffer, aOffset, len);
			mTotalLength += len;
			aOffset += len;
			aLength -= len;

			if (mBuffer.remaining() == 0)
			{
				flushBlock();
			}
		}
	}


	@Override
	public void close() throws IOException
	{
		if (mClosed)
		{
			return;
		}

		Log.d("closing blob");
		Log.inc();

		if (mBuffer.position() > 0)
		{
			flushBlock();
		}

		int pointerBufferLength = mPointerBuffer.position();

		if (pointerBufferLength > POINTER_MAX_LENGTH) // create indirect block if pointer buffer length exceed limit
		{
			BlockPointer bp = mBlockAccessor.writeBlock(mPointerBuffer.array(), 0, pointerBufferLength, mTransactionId.get(), BlockType.BLOB_INDEX, 0);
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
			mOnCloseListener.onClose(this, mHeader);
		}

		Log.dec();
	}


	public byte[] finish() throws IOException
	{
		close();

		return mHeader.clone();
	}


	private void flushBlock() throws IOException
	{
		BlockPointer bp = mBlockAccessor.writeBlock(mBuffer.array(), 0, mBuffer.position(), mTransactionId.get(), BlockType.BLOB_DATA, 0);
		bp.marshal(mPointerBuffer);

		mBuffer.position(0);

//		Arrays.fill(mBuffer.array(), (byte)0);
	}


	@FunctionalInterface
	public interface OnCloseListener
	{
		void onClose(BlobOutputStream aBlobOutputStream, byte[] aHeader);
	}
}
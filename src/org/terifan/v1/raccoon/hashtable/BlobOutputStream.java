package org.terifan.v1.raccoon.hashtable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import org.terifan.v1.util.Varint;
import org.terifan.v1.raccoon.io.IBlockDevice;
import org.terifan.v1.util.ByteArray;
import org.terifan.v1.security.ISAAC;


class BlobOutputStream extends OutputStream
{
	private HashTable mHashTable;
	private ByteArrayOutputStream mFragmentBuffer;
	private byte[] mOutput;
	private int mFragmentCount;
	private int mBytesWritten;
	private byte[] mBuffer;
	private int mBufferOffset;
	private int mBlockSize;
	private long mBlockKey;
	private long mTransactionId;


	public BlobOutputStream(HashTable aHashTable)
	{
		mHashTable = aHashTable;
		mTransactionId = mHashTable.getTransactionId();
		mBlockSize = mHashTable.getBlockDevice().getBlockSize();
		mBuffer = new byte[Blob.MAX_ADJACENT_BLOCKS * mBlockSize];
		mBlockKey = 0xffffffffL & ISAAC.PRNG.nextInt();

		mFragmentBuffer = new ByteArrayOutputStream();
	}


	public byte[] getOutput()
	{
		assert mOutput != null;

		return mOutput;
	}


	@Override
	public void write(int b) throws IOException
	{
		mBuffer[mBufferOffset++] = (byte)b;

		if (mBufferOffset == mBuffer.length)
		{
			flushBuffer();
		}
	}


	@Override
	public void close() throws IOException
	{
		if (mOutput == null)
		{
			int totalLength = mBytesWritten + mBufferOffset;

			// pad buffer
			write(new byte[(mBlockSize - (mBufferOffset % mBlockSize)) % mBlockSize]);

			while (mBufferOffset > 0)
			{
				flushBuffer();
			}

			// create indirect block if pointers exceed max length
			if (mFragmentBuffer.size() > Blob.POINTER_MAX_LENGTH)
			{
				// pad buffer
				mFragmentBuffer.write(new byte[(mBlockSize - (mFragmentBuffer.size() % mBlockSize)) % mBlockSize]);

				IBlockDevice blockDevice = mHashTable.getBlockDevice();
				byte[] output = mFragmentBuffer.toByteArray();
				int blockCount = (output.length + mBlockSize - 1) / mBlockSize;
				int blockIndex = (int)blockDevice.allocBlock(blockCount);

				if (blockIndex < 0)
				{
					throw new IOException("Unsufficient space in block device.");
				}

//				mHashTable.d("Write indirect block at " + blockIndex + " +" + blockCount);

				blockDevice.writeBlock(blockIndex, output, 0, mBlockSize * blockCount, (mTransactionId << 32) | mBlockKey);

				mFragmentBuffer.reset();

				Varint.put(mFragmentBuffer, blockIndex);
				Varint.put(mFragmentBuffer, blockCount - 1);

				mFragmentCount = -mFragmentCount; // signal that there is a indirect block
			}

			mOutput = new byte[Blob.HEADER_SIZE + mFragmentBuffer.size()];
			ByteArray.putInt(mOutput, Blob.HEADER_FIELD_LENGTH, totalLength);
			ByteArray.putInt(mOutput, Blob.HEADER_FIELD_COUNT, mFragmentCount);
			ByteArray.putInt(mOutput, Blob.HEADER_FIELD_TRANSACTION, (int)mTransactionId);
			ByteArray.putInt(mOutput, Blob.HEADER_FIELD_BLOCK_KEY, (int)mBlockKey);
			ByteArray.put(mOutput, Blob.HEADER_SIZE, mFragmentBuffer.toByteArray());
		}
	}


	private void flushBuffer() throws IOException
	{
		IBlockDevice blockDevice = mHashTable.getBlockDevice();

//		Result<Integer> blockAlloc = new Result<>((mBufferOffset + mBlockSize - 1) / mBlockSize);
//		int blockIndex = (int)blockDevice.allocBlock(blockAlloc);
//		int blockCount = blockAlloc.get();
		int blockCount = (mBufferOffset + mBlockSize - 1) / mBlockSize;
		int blockIndex = (int)blockDevice.allocBlock(blockCount);
		int len = blockCount * mBlockSize;

		if (blockIndex < 0)
		{
			throw new IOException("Unsufficient space in block device.");
		}

		blockDevice.writeBlock(blockIndex, mBuffer, 0, blockCount * mBlockSize, (mTransactionId << 32) | mBlockKey);

		Varint.put(mFragmentBuffer, blockIndex);
		Varint.put(mFragmentBuffer, blockCount - 1);

		mBytesWritten += len;
		mFragmentCount++;

//		mHashTable.d("Write fragment " + mFragmentCount + " at " + blockIndex + " +" + blockCount);

		if (len < mBufferOffset)
		{
			System.arraycopy(mBuffer, len, mBuffer, 0, mBufferOffset);
		}

		mBufferOffset -= len;
	}
}

package org.terifan.raccoon.io;

import java.io.IOException;
import java.util.Arrays;
import java.util.TreeMap;
import org.terifan.raccoon.util.Log;


public class MemoryBlockDevice implements IPhysicalBlockDevice
{
	private TreeMap<Long, byte[]> mStorage = new TreeMap<>();
	private int mBlockSize;


	public MemoryBlockDevice(int aBlockSize)
	{
		mBlockSize = aBlockSize;
	}


	public TreeMap<Long, byte[]> getStorage()
	{
		return mStorage;
	}


	@Override
	public void writeBlock(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, long aBlockKey) throws IOException
	{
		while (aBufferLength > 0)
		{
			Log.v("write block " + aBlockIndex + " +" + (aBufferLength/mBlockSize));

			mStorage.put(aBlockIndex, Arrays.copyOfRange(aBuffer, aBufferOffset, aBufferOffset + mBlockSize));

			aBlockIndex++;
			aBufferOffset += mBlockSize;
			aBufferLength -= mBlockSize;
		}
	}


	@Override
	public void readBlock(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, long aBlockKey) throws IOException
	{
		while (aBufferLength > 0)
		{
			Log.v("read block  " + aBlockIndex + " +" + aBufferLength/mBlockSize);

			byte[] block = mStorage.get(aBlockIndex);

			if (block != null)
			{
				System.arraycopy(block, 0, aBuffer, aBufferOffset, mBlockSize);
			}
			else
			{
				throw new IOException("Reading a free block: " + aBlockIndex);
			}

			aBlockIndex++;
			aBufferOffset += mBlockSize;
			aBufferLength -= mBlockSize;
		}
	}


	@Override
	public void commit(boolean aMetadata) throws IOException
	{
	}


	@Override
	public void close() throws IOException
	{
	}


	@Override
	public int getBlockSize()
	{
		return mBlockSize;
	}


	@Override
	public long length()
	{
		return mStorage.isEmpty() ? 0L : mStorage.lastKey() + 1;
	}
}
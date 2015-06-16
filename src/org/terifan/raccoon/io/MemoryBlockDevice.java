package org.terifan.raccoon.io;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.terifan.raccoon.util.Log;
import static tests.__TestUtils.b;


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
		Log.v("write block " + aBlockIndex + " +" + (aBufferLength / mBlockSize));

		while (aBufferLength > 0)
		{
			mStorage.put(aBlockIndex, Arrays.copyOfRange(aBuffer, aBufferOffset, aBufferOffset + mBlockSize));

			aBlockIndex++;
			aBufferOffset += mBlockSize;
			aBufferLength -= mBlockSize;
		}
	}


	@Override
	public void readBlock(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, long aBlockKey) throws IOException
	{
		Log.v("read block  " + aBlockIndex + " +" + aBufferLength/mBlockSize);

		while (aBufferLength > 0)
		{
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


	public void dump()
	{
		for (Entry<Long,byte[]> entry : mStorage.entrySet())
		{
			Log.out.println("Block Index " + entry.getKey() + ":");
			Log.hexDump(entry.getValue());
		}
	}
}

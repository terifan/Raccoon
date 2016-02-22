package org.terifan.raccoon.io;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;


class CachingBlockDevice
{
	private final static int LIMIT = 1024;

	private IPhysicalBlockDevice mBlockDevice;
	private HashMap<Long, CacheEntry> mCache;
	private LinkedList<Long> mCacheOrder;


	public CachingBlockDevice(IPhysicalBlockDevice aBlockDevice)
	{
		mBlockDevice = aBlockDevice;
		mCache = new HashMap<>();
		mCacheOrder = new LinkedList<>();
	}


	public void free(long aBlockIndex)
	{
		mCache.remove(aBlockIndex);
		mCacheOrder.remove(aBlockIndex);
	}


	public void writeBlock(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, long aBlockKey) throws IOException
	{
		CacheEntry entry = new CacheEntry(aBlockIndex, Arrays.copyOfRange(aBuffer, aBufferOffset, aBufferOffset + aBufferLength), aBlockKey);

		mCache.put(aBlockIndex, entry);

		mCacheOrder.remove(aBlockIndex);
		mCacheOrder.addFirst(aBlockIndex);

		if (mCache.size() > LIMIT)
		{
			reduce();
		}
	}


	public void readBlock(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, long aBlockKey) throws IOException
	{
		CacheEntry entry = mCache.get(aBlockIndex);

		if (entry != null)
		{
			assert entry.mBuffer.length == aBufferLength;
			assert entry.mBlockKey == aBlockKey;

			System.arraycopy(entry.mBuffer, 0, aBuffer, aBufferOffset, aBufferLength);
		}
		else
		{
			mBlockDevice.readBlock(aBlockIndex, aBuffer, aBufferOffset, aBufferLength, aBlockKey);
		}
	}


	public void clear()
	{
		mCache.clear();
		mCacheOrder.clear();
	}


	private void reduce() throws IOException
	{
		for (int i = 0; i < LIMIT / 2; i++)
		{
			CacheEntry entry = mCache.remove(mCacheOrder.removeLast());

			mBlockDevice.writeBlock(entry.mBlockIndex, entry.mBuffer, 0, entry.mBuffer.length, entry.mBlockKey);
		}
	}


	public void flush() throws IOException
	{
		for (CacheEntry entry : mCache.values())
		{
			mBlockDevice.writeBlock(entry.mBlockIndex, entry.mBuffer, 0, entry.mBuffer.length, entry.mBlockKey);
		}

		mCache.clear();
		mCacheOrder.clear();
	}


	private static class CacheEntry
	{
		long mBlockIndex;
		long mBlockKey;
		byte[] mBuffer;


		public CacheEntry(long aBlockIndex, byte[] aBuffer, long aKey)
		{
			mBlockIndex = aBlockIndex;
			mBuffer = aBuffer;
			mBlockKey = aKey;
		}
	}
}

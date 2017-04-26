package org.terifan.raccoon.io.managed;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import org.terifan.raccoon.Constants;
import org.terifan.raccoon.io.physical.IPhysicalBlockDevice;
import static org.terifan.raccoon.PerformanceCounters.*;


class LazyWriteCache
{
	private final IPhysicalBlockDevice mBlockDevice;
	private final HashMap<Long, CacheEntry> mCache;
	private final LinkedList<Long> mCacheOrder;
	private final int mCapacity;


	public LazyWriteCache(IPhysicalBlockDevice aBlockDevice, int aCapacity)
	{
		mBlockDevice = aBlockDevice;
		mCache = new HashMap<>();
		mCacheOrder = new LinkedList<>();
		mCapacity = aCapacity;
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

		if (Constants.REORDER_LAZY_CACHE_ON_WRITE)
		{
			mCacheOrder.remove(aBlockIndex);
			mCacheOrder.addFirst(aBlockIndex);
		}

		if (mCache.size() > mCapacity)
		{
			assert increment(LAZY_WRITE_CACHE_FLUSH);

			for (int i = 0; i < mCapacity / 2; i++)
			{
				entry = mCache.remove(mCacheOrder.removeLast());

				mBlockDevice.writeBlock(entry.mBlockIndex, entry.mBuffer, 0, entry.mBuffer.length, entry.mBlockKey);
			}
		}
	}


	public void readBlock(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, long aBlockKey) throws IOException
	{
		CacheEntry entry = mCache.get(aBlockIndex);

		if (entry != null)
		{
			assert entry.mBuffer.length == aBufferLength : entry.mBuffer.length+" == "+aBufferLength;
			assert entry.mBlockKey == aBlockKey : entry.mBlockKey+" == "+aBlockKey;

			System.arraycopy(entry.mBuffer, 0, aBuffer, aBufferOffset, aBufferLength);

			assert increment(LAZY_WRITE_CACHE_HIT);

			if (Constants.REORDER_LAZY_CACHE_ON_READ)
			{
				synchronized (this)
				{
					mCacheOrder.remove(aBlockIndex);
					mCacheOrder.addFirst(aBlockIndex);
				}
			}
		}
		else
		{
			assert increment(LAZY_WRITE_CACHE_READ);

			mBlockDevice.readBlock(aBlockIndex, aBuffer, aBufferOffset, aBufferLength, aBlockKey);
		}
	}


	public void clear()
	{
		mCache.clear();
		mCacheOrder.clear();
	}


	public void flush() throws IOException
	{
		for (CacheEntry entry : mCache.values())
		{
			mBlockDevice.writeBlock(entry.mBlockIndex, entry.mBuffer, 0, entry.mBuffer.length, entry.mBlockKey);
		}

		clear();
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
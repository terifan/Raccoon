package org.terifan.raccoon.io.managed;

import java.io.IOException;
import java.util.HashSet;
import org.terifan.raccoon.DatabaseException;
import org.terifan.raccoon.io.physical.IPhysicalBlockDevice;
import org.terifan.raccoon.util.Log;


public class ManagedBlockDevice implements IManagedBlockDevice, AutoCloseable
{
	private final static int RESERVED_BLOCKS = 2;

	private IPhysicalBlockDevice mBlockDevice;
	private RangeMap mRangeMap;
	private RangeMap mPendingRangeMap;
	private SuperBlock mSuperBlock;
	private HashSet<Integer> mUncommittedAllocations;
	private String mBlockDeviceLabel;
	private int mBlockSize;
	private boolean mModified;
	private boolean mWasCreated;
	private boolean mDoubleCommit;
	private LazyWriteCache mLazyWriteCache;


	/**
	 * Create/open a ManagedBlockDevice with an user defined label.
	 *
	 * @param aBlockDeviceLabel
	 *   a label describing contents of the block device. If a non-null value is provided then this value must match the value found inside
	 *   the block device opened or an exception is thrown.
	 */
	public ManagedBlockDevice(IPhysicalBlockDevice aBlockDevice, String aBlockDeviceLabel, int aLazyWriteCacheSize) throws IOException
	{
		if (aBlockDevice.getBlockSize() < 512)
		{
			throw new IllegalArgumentException("Block device must have 512 byte block size or larger.");
		}

		mBlockDevice = aBlockDevice;
		mBlockDeviceLabel = aBlockDeviceLabel;
		mBlockSize = aBlockDevice.getBlockSize();
		mWasCreated = mBlockDevice.length() < RESERVED_BLOCKS;
		mUncommittedAllocations = new HashSet<>();
		mDoubleCommit = true;
		mLazyWriteCache = new LazyWriteCache(mBlockDevice, aLazyWriteCacheSize);

		init();
	}


	private void init() throws IOException
	{
		if (mWasCreated)
		{
			createBlockDevice();
		}
		else
		{
			loadBlockDevice();
		}
	}


	private void createBlockDevice() throws IOException, IllegalStateException
	{
		Log.i("create block device");
		Log.inc();

		mRangeMap = new RangeMap();
		mRangeMap.add(0, Integer.MAX_VALUE);

		mPendingRangeMap = mRangeMap.clone();

		mSuperBlock = new SuperBlock();
		mSuperBlock.mWriteCounter = -1L; // counter is incremented in writeSuperBlock method and we want to ensure we write block 0 before block 1
		mSuperBlock.mBlockDeviceLabel = mBlockDeviceLabel == null ? "" : mBlockDeviceLabel;

		setExtraData(null);

		long index = allocBlockInternal(2);

		if (index != 0)
		{
			throw new IllegalStateException("The super block must be located at block index 0, was: " + index);
		}

		// write two copies of super block
		writeSuperBlock();
		writeSuperBlock();

		Log.dec();
	}


	private void loadBlockDevice() throws IOException
	{
		Log.i("load block device");
		Log.inc();

		readSuperBlock();

		mRangeMap = new SpaceMapIO().readSpaceMap(mSuperBlock, this, mBlockDevice);
		mPendingRangeMap = mRangeMap.clone();

		Log.dec();
	}


	public void setDoubleCommit(boolean aDoubleCommit)
	{
		mDoubleCommit = aDoubleCommit;
	}


	public boolean isDoubleCommit()
	{
		return mDoubleCommit;
	}


	@Override
	public boolean isModified()
	{
		return mModified;
	}


	@Override
	public long length() throws IOException
	{
		return mBlockDevice.length() - RESERVED_BLOCKS;
	}


	@Override
	public void close() throws IOException
	{
		if (mModified)
		{
			rollback();
		}

		mLazyWriteCache = null;

		if (mBlockDevice != null)
		{
			mBlockDevice.close();
			mBlockDevice = null;
		}
	}


	@Override
	public void forceClose() throws IOException
	{
		mLazyWriteCache = null;

		mUncommittedAllocations.clear();

		if (mBlockDevice != null)
		{
			mBlockDevice.forceClose();
			mBlockDevice = null;
		}
	}


	@Override
	public int getBlockSize()
	{
		return mBlockSize;
	}


	@Override
	public long allocBlock(int aBlockCount) throws IOException
	{
		long blockIndex = allocBlockInternal(aBlockCount) - RESERVED_BLOCKS;

		if (blockIndex < 0)
		{
			throw new IOException("Illegal block index allocated.");
		}

		return blockIndex;
	}


	long allocBlockInternal(int aBlockCount) throws IOException
	{
		int blockIndex = mRangeMap.next(aBlockCount);

		if (blockIndex < 0)
		{
			return -1;
		}

		Log.d("alloc block %d +%d", blockIndex, aBlockCount);

		mModified = true;

		for (int i = 0; i < aBlockCount; i++)
		{
			mUncommittedAllocations.add(blockIndex + i);
		}

		mPendingRangeMap.remove(blockIndex, aBlockCount);

		return blockIndex;
	}


	@Override
	public void freeBlock(long aBlockIndex, int aBlockCount) throws IOException
	{
		if (aBlockIndex < 0)
		{
			throw new IOException("Illegal offset: " + aBlockIndex);
		}

		freeBlockInternal(RESERVED_BLOCKS + aBlockIndex, aBlockCount);
	}


	void freeBlockInternal(long aBlockIndex, int aBlockCount) throws IOException
	{
		Log.d("free block %d +%d", aBlockIndex, aBlockCount);

		mModified = true;

		mLazyWriteCache.free(aBlockIndex);

		int blockIndex = (int)aBlockIndex;

		for (int i = 0; i < aBlockCount; i++)
		{
			if (mUncommittedAllocations.remove(blockIndex + i))
			{
				mRangeMap.add(blockIndex + i, 1);
			}
		}

		mPendingRangeMap.add(blockIndex, aBlockCount);
	}


	@Override
	public void writeBlock(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, long aIV0, long aIV1) throws IOException
	{
		if (aBlockIndex < 0)
		{
			throw new IOException("Illegal offset: " + aBlockIndex);
		}
		if ((aBufferLength % mBlockSize) != 0)
		{
			throw new IOException("Illegal buffer length: " + aBlockIndex);
		}

		writeBlockInternal(RESERVED_BLOCKS + aBlockIndex, aBuffer, aBufferOffset, aBufferLength, aIV0, aIV1);
	}


	private void writeBlockInternal(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, long aIV0, long aIV1) throws IOException
	{
		assert aBufferLength > 0;
		assert (aBufferLength % mBlockSize) == 0;

		if (!mRangeMap.isFree((int)aBlockIndex, aBufferLength / mBlockSize))
		{
			throw new IOException("Range not allocted: " + aBlockIndex + " +" + (aBufferLength / mBlockSize));
		}

		Log.d("write block %d +%d", aBlockIndex, aBufferLength / mBlockSize);
		Log.inc();

		mModified = true;

		mLazyWriteCache.writeBlock(aBlockIndex, aBuffer, aBufferOffset, aBufferLength, aIV0, aIV1);

		Log.dec();
	}


	@Override
	public void readBlock(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, long aIV0, long aIV1) throws IOException
	{
		if (aBlockIndex < 0)
		{
			throw new IOException("Illegal offset: " + aBlockIndex);
		}
		if ((aBufferLength % mBlockSize) != 0)
		{
			throw new IOException("Illegal buffer length: " + aBlockIndex);
		}

		readBlockInternal(aBlockIndex + RESERVED_BLOCKS, aBuffer, aBufferOffset, aBufferLength, aIV0, aIV1);
	}


	private void readBlockInternal(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, long aIV0, long aIV1) throws IOException
	{
		assert aBufferLength > 0;
		assert (aBufferLength % mBlockSize) == 0;

		if (!mRangeMap.isFree((int)aBlockIndex, aBufferLength / mBlockSize))
		{
			throw new IOException("Range not allocted: " + aBlockIndex + " +" + (aBufferLength / mBlockSize));
		}

		Log.d("read block %d +%d", aBlockIndex, aBufferLength/mBlockSize);
		Log.inc();

		mLazyWriteCache.readBlock(aBlockIndex, aBuffer, aBufferOffset, aBufferLength, aIV0, aIV1);

		Log.dec();
	}


	@Override
	public void commit(boolean aMetadata) throws IOException
	{
		if (mModified)
		{
			mLazyWriteCache.flush();

			Log.i("committing managed block device");
			Log.inc();

			new SpaceMapIO().writeSpaceMap(mSuperBlock, mPendingRangeMap, this, mBlockDevice);

			if (mDoubleCommit) // enabled by default
			{
				// commit twice since write operations on disk may occur out of order ie. superblock may be written before spacemap even
				// tough calls made in reverse order
				mBlockDevice.commit(false);
			}

			writeSuperBlock();

			mBlockDevice.commit(aMetadata);

			mUncommittedAllocations.clear();
			mRangeMap = mPendingRangeMap.clone();
			mWasCreated = false;
			mModified = false;

			Log.dec();
		}
	}


	@Override
	public void commit() throws IOException
	{
		commit(false);
	}


	@Override
	public void rollback() throws IOException
	{
		if (mModified)
		{
			Log.i("rollbacking block device");
			Log.inc();

			mLazyWriteCache.clear();

			mUncommittedAllocations.clear();

			mPendingRangeMap = mRangeMap.clone();

			init();

			mModified = false;

			Log.dec();
		}
	}


	private void readSuperBlock() throws IOException
	{
		Log.d("read super block");
		Log.inc();

		SuperBlock superBlockOne = new SuperBlockIO().readSuperBlock(mBlockDevice, 0L);
		SuperBlock superBlockTwo = new SuperBlockIO().readSuperBlock(mBlockDevice, 1L);

		if (!(mBlockDeviceLabel == null || mBlockDeviceLabel.isEmpty() && superBlockOne.mBlockDeviceLabel.isEmpty() || mBlockDeviceLabel.equals(superBlockOne.mBlockDeviceLabel)))
		{
			throw new UnsupportedVersionException("Block device label don't match: was " + (superBlockOne.mBlockDeviceLabel.isEmpty()?"<empty>" : superBlockOne.mBlockDeviceLabel) + ", expected " + (mBlockDeviceLabel.isEmpty()?"<empty>" : mBlockDeviceLabel));
		}

		if (superBlockOne.mWriteCounter == superBlockTwo.mWriteCounter + 1)
		{
			mSuperBlock = superBlockOne;

			Log.d("using super block 0");
		}
		else if (superBlockTwo.mWriteCounter == superBlockOne.mWriteCounter + 1)
		{
			mSuperBlock = superBlockTwo;

			Log.d("using super block 1");
		}
		else
		{
			throw new DatabaseException("Database appears to be corrupt. SuperBlock versions are illegal: " + superBlockOne.mWriteCounter + " / " + superBlockTwo.mWriteCounter);
		}

		Log.dec();
	}


	private void writeSuperBlock() throws IOException
	{
		mSuperBlock.mWriteCounter++;

		Log.i("write super block %d", mSuperBlock.mWriteCounter & 1L);
		Log.inc();

		long pageIndex = mSuperBlock.mWriteCounter & 1L;

		new SuperBlockIO().writeSuperBlock(mBlockDevice, pageIndex, mSuperBlock);

		Log.dec();
	}


	@Override
	public void setExtraData(byte[] aExtraData)
	{
		Log.i("set extra data");

		if (aExtraData != null && aExtraData.length > EXTRA_DATA_LIMIT)
		{
			throw new IllegalArgumentException("Length of extra data exceeds maximum length: extra length: " + aExtraData.length + ", limit: " + EXTRA_DATA_LIMIT);
		}

		mModified = true;

		mSuperBlock.mExtraData = aExtraData == null ? null : aExtraData.clone();

//		mSuperBlock.mExtraDataModified = true;
	}


	@Override
	public byte[] getExtraData()
	{
		return mSuperBlock.mExtraData == null ? null : mSuperBlock.mExtraData.clone();
	}


	@Override
	public String toString()
	{
		return mRangeMap.toString();
	}


	/**
	 * Return the space map layout as a String (ranges of free blocks). If the space map is fragmented this may be a long String.
	 */
	public String getSpaceMap()
	{
		return mRangeMap.toString();
	}


	/**
	 * Return the maximum available space this block device can theoretically allocate. This value may be greater than what the underlying block device can support.
	 */
	public long getAvailableSpace() throws IOException
	{
		return mRangeMap.getFreeSpace();
	}


	/**
	 * Return the size of the underlying block device, ie. size of a file acting as a block storage.
	 */
	public long getAllocatedSpace() throws IOException
	{
		return mBlockDevice.length();
	}


	/**
	 * Return the free space within the allocated space.
	 */
	public long getFreeSpace() throws IOException
	{
		return mBlockDevice.length() - mRangeMap.getUsedSpace();
	}


	/**
	 * Return the number of blocks actually used.
	 */
	public long getUsedSpace() throws IOException
	{
		return mRangeMap.getUsedSpace();
	}


	@Override
	public void setLength(long aNewLength) throws IOException
	{
		mBlockDevice.setLength(aNewLength + RESERVED_BLOCKS);
	}


	@Override
	public void clear() throws IOException
	{
		mBlockDevice.setLength(0);

		mUncommittedAllocations.clear();

		createBlockDevice();
	}
}

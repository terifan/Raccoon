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
	private SuperBlock mSuperBlock;
	private String mBlockDeviceLabel;
	private int mBlockSize;
	private boolean mModified;
	private boolean mWasCreated;
	private boolean mDoubleCommit;
	private LazyWriteCache mLazyWriteCache;
	private SpaceMap mSpaceMap;


	/**
	 * Create/open a ManagedBlockDevice with an user defined label.
	 *
	 * @param aBlockDeviceLabel
	 * a label describing contents of the block device. If a non-null value is provided then this value must match the value found inside
	 * the block device opened or an exception is thrown.
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
		mLazyWriteCache = new LazyWriteCache(mBlockDevice, aLazyWriteCacheSize);
		mDoubleCommit = true;

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

		mSpaceMap = new SpaceMap();

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

		mSpaceMap = new SpaceMap(mSuperBlock, this, mBlockDevice);

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

		mSpaceMap.clearUncommitted();

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
		mModified = true;

		return mSpaceMap.alloc(aBlockCount);
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

		mSpaceMap.free(aBlockIndex, aBlockCount);

		mLazyWriteCache.free(aBlockIndex);
	}


	@Override
	public void writeBlock(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, long[] aIV) throws IOException
	{
		if (aBlockIndex < 0)
		{
			throw new IOException("Illegal offset: " + aBlockIndex);
		}
		if ((aBufferLength % mBlockSize) != 0)
		{
			throw new IOException("Illegal buffer length: " + aBlockIndex);
		}

		writeBlockInternal(RESERVED_BLOCKS + aBlockIndex, aBuffer, aBufferOffset, aBufferLength, aIV);
	}


	private void writeBlockInternal(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, long[] aIV) throws IOException
	{
		assert aBufferLength > 0;
		assert (aBufferLength % mBlockSize) == 0;

		mSpaceMap.assertUsed(aBlockIndex, aBufferLength / mBlockSize);

		mModified = true;

		Log.d("write block %d +%d", aBlockIndex, aBufferLength / mBlockSize);
		Log.inc();

		mLazyWriteCache.writeBlock(aBlockIndex, aBuffer, aBufferOffset, aBufferLength, aIV);

		Log.dec();
	}


	@Override
	public void readBlock(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, long[] aIV) throws IOException
	{
		if (aBlockIndex < 0)
		{
			throw new IOException("Illegal offset: " + aBlockIndex);
		}
		if ((aBufferLength % mBlockSize) != 0)
		{
			throw new IOException("Illegal buffer length: " + aBlockIndex);
		}

		readBlockInternal(aBlockIndex + RESERVED_BLOCKS, aBuffer, aBufferOffset, aBufferLength, aIV);
	}


	private void readBlockInternal(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, long[] aIV) throws IOException
	{
		assert aBufferLength > 0;
		assert (aBufferLength % mBlockSize) == 0;

		mSpaceMap.assertUsed(aBlockIndex, aBufferLength / mBlockSize);

		Log.d("read block %d +%d", aBlockIndex, aBufferLength / mBlockSize);
		Log.inc();

		mLazyWriteCache.readBlock(aBlockIndex, aBuffer, aBufferOffset, aBufferLength, aIV);

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

			mSpaceMap.write(mSuperBlock.mSpaceMapPointer, this, mBlockDevice);

			if (mDoubleCommit) // enabled by default
			{
				// commit twice since write operations on disk may occur out of order ie. superblock may be written before spacemap even
				// tough calls made in reverse order
				mBlockDevice.commit(false);
			}

			writeSuperBlock();

			mBlockDevice.commit(aMetadata);

			mSpaceMap.clearUncommitted();
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

			mSpaceMap.clearUncommitted();

			mSpaceMap.rollback();

			init();

			mModified = false;

			Log.dec();
		}
	}


	private void readSuperBlock() throws IOException
	{
		Log.d("read super block");
		Log.inc();

		SuperBlock superBlockOne = new SuperBlock(mBlockDevice, 0L);
		SuperBlock superBlockTwo = new SuperBlock(mBlockDevice, 1L);

		if (!(mBlockDeviceLabel == null || mBlockDeviceLabel.isEmpty() && superBlockOne.mBlockDeviceLabel.isEmpty() || mBlockDeviceLabel.equals(superBlockOne.mBlockDeviceLabel)))
		{
			throw new UnsupportedVersionException("Block device label don't match: was " + (superBlockOne.mBlockDeviceLabel.isEmpty() ? "<empty>" : superBlockOne.mBlockDeviceLabel) + ", expected " + (mBlockDeviceLabel.isEmpty() ? "<empty>" : mBlockDeviceLabel));
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

		mSuperBlock.write(mBlockDevice, pageIndex);

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
		return mSpaceMap.getRangeMap().toString();
	}


	/**
	 * Return the space map layout as a String (ranges of free blocks). If the space map is fragmented this may be a long String.
	 */
	public String getSpaceMap()
	{
		return mSpaceMap.getRangeMap().toString();
	}


	/**
	 * Return the maximum available space this block device can theoretically allocate. This value may be greater than what the underlying block device can support.
	 */
	public long getAvailableSpace() throws IOException
	{
		return mSpaceMap.getRangeMap().getFreeSpace();
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
		return mBlockDevice.length() - mSpaceMap.getRangeMap().getUsedSpace();
	}


	/**
	 * Return the number of blocks actually used.
	 */
	public long getUsedSpace() throws IOException
	{
		return mSpaceMap.getRangeMap().getUsedSpace();
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

		mSpaceMap.clearUncommitted();

		createBlockDevice();
	}
}

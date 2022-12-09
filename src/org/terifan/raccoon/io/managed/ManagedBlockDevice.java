package org.terifan.raccoon.io.managed;

import org.terifan.bundle.Document;
import org.terifan.raccoon.DatabaseException;
import org.terifan.raccoon.io.DatabaseIOException;
import org.terifan.raccoon.io.physical.IPhysicalBlockDevice;
import org.terifan.raccoon.util.Log;


public class ManagedBlockDevice implements IManagedBlockDevice, AutoCloseable
{
	private final static int RESERVED_BLOCKS = 2;

	private IPhysicalBlockDevice mBlockDevice;
	private SuperBlock mSuperBlock;
	private int mBlockSize;
	private boolean mModified;
	private boolean mWasCreated;
	private boolean mDoubleCommit;
	private SpaceMap mSpaceMap;


	public ManagedBlockDevice(IPhysicalBlockDevice aBlockDevice)
	{
		if (aBlockDevice.getBlockSize() < 512)
		{
			throw new IllegalArgumentException("The block device must have 512 byte block size or larger.");
		}

		mBlockDevice = aBlockDevice;
		mBlockSize = aBlockDevice.getBlockSize();
		mWasCreated = mBlockDevice.length() < RESERVED_BLOCKS;
		mDoubleCommit = true;

		init();
	}


	private void init()
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


	private void createBlockDevice()
	{
		Log.i("create block device");
		Log.inc();

		mSpaceMap = new SpaceMap();

		mSuperBlock = new SuperBlock(-1L); // counter is incremented in writeSuperBlock method and we want to ensure we write block 0 before block 1

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


	private void loadBlockDevice()
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
	public Document getApplicationHeader()
	{
		return mSuperBlock.getApplicationHeader();
	}


	@Override
	public long getTransactionId()
	{
		return mSuperBlock.getTransactionId();
	}


	@Override
	public long length()
	{
		return mBlockDevice.length() - RESERVED_BLOCKS;
	}


	@Override
	public void close()
	{
		if (mModified)
		{
			rollback();
		}

		if (mBlockDevice != null)
		{
			mBlockDevice.close();
			mBlockDevice = null;
		}
	}


	@Override
	public void forceClose()
	{
		mSpaceMap.reset();

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
	public long allocBlock(int aBlockCount)
	{
		long blockIndex = allocBlockInternal(aBlockCount) - RESERVED_BLOCKS;

		if (blockIndex < 0)
		{
			throw new DatabaseIOException("Illegal block index allocated.");
		}

		return blockIndex;
	}


	long allocBlockInternal(int aBlockCount)
	{
		mModified = true;

		return mSpaceMap.alloc(aBlockCount);
	}


	@Override
	public void freeBlock(long aBlockIndex, int aBlockCount)
	{
		if (aBlockIndex < 0)
		{
			throw new DatabaseIOException("Illegal offset: " + aBlockIndex);
		}

		freeBlockInternal(RESERVED_BLOCKS + aBlockIndex, aBlockCount);
	}


	void freeBlockInternal(long aBlockIndex, int aBlockCount)
	{
		Log.d("free block %d +%d", aBlockIndex, aBlockCount);

		mModified = true;

		mSpaceMap.free(aBlockIndex, aBlockCount);
	}


	@Override
	public void writeBlock(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, long[] aBlockKey)
	{
		if (aBlockIndex < 0)
		{
			throw new DatabaseIOException("Illegal offset: " + aBlockIndex);
		}
		if ((aBufferLength % mBlockSize) != 0)
		{
			throw new DatabaseIOException("Illegal buffer length: " + aBlockIndex);
		}

		writeBlockInternal(RESERVED_BLOCKS + aBlockIndex, aBuffer, aBufferOffset, aBufferLength, aBlockKey);
	}


	private void writeBlockInternal(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, long[] aBlockKey)
	{
		assert aBufferLength > 0;
		assert (aBufferLength % mBlockSize) == 0;

		mSpaceMap.assertUsed(aBlockIndex, aBufferLength / mBlockSize);

		mModified = true;

		Log.d("write block %d +%d", aBlockIndex, aBufferLength / mBlockSize);
		Log.inc();

		mBlockDevice.writeBlock(aBlockIndex, aBuffer, aBufferOffset, aBufferLength, aBlockKey);

		Log.dec();
	}


	@Override
	public void readBlock(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, long[] aBlockKey)
	{
		if (aBlockIndex < 0)
		{
			throw new DatabaseIOException("Illegal offset: " + aBlockIndex);
		}
		if ((aBufferLength % mBlockSize) != 0)
		{
			throw new DatabaseIOException("Illegal buffer length: " + aBlockIndex);
		}

		readBlockInternal(aBlockIndex + RESERVED_BLOCKS, aBuffer, aBufferOffset, aBufferLength, aBlockKey);
	}


	private void readBlockInternal(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, long[] aBlockKey)
	{
		assert aBufferLength > 0;
		assert (aBufferLength % mBlockSize) == 0;

		mSpaceMap.assertUsed(aBlockIndex, aBufferLength / mBlockSize);

		Log.d("read block %d +%d", aBlockIndex, aBufferLength / mBlockSize);
		Log.inc();

		mBlockDevice.readBlock(aBlockIndex, aBuffer, aBufferOffset, aBufferLength, aBlockKey);

		Log.dec();
	}


	@Override
	public void commit(boolean aMetadata)
	{
		if (mModified)
		{
			Log.i("committing managed block device");
			Log.inc();

			mSpaceMap.write(mSuperBlock.getSpaceMapPointer(), this, mBlockDevice);

			if (mDoubleCommit) // enabled by default
			{
				// commit twice since write operations on disk may occur out of order ie. superblock may be written before spacemap even
				// tough calls made in reverse order
				mBlockDevice.commit(false);
			}

			writeSuperBlock();

			mBlockDevice.commit(aMetadata);

			mSpaceMap.reset();
			mWasCreated = false;
			mModified = false;

			Log.dec();
		}
	}


	@Override
	public void commit()
	{
		commit(false);
	}


	@Override
	public void rollback()
	{
		if (mModified)
		{
			Log.i("rollbacking block device");
			Log.inc();

			mSpaceMap.reset();
			mSpaceMap.rollback();

			init();

			mModified = false;

			Log.dec();
		}
	}


	private void readSuperBlock()
	{
		Log.d("read super block");
		Log.inc();

		SuperBlock superBlockOne = new SuperBlock(mBlockDevice, 0L);
		SuperBlock superBlockTwo = new SuperBlock(mBlockDevice, 1L);

		if (superBlockOne.getTransactionId() == superBlockTwo.getTransactionId() + 1)
		{
			mSuperBlock = superBlockOne;

			Log.d("using super block 0");
		}
		else if (superBlockTwo.getTransactionId() == superBlockOne.getTransactionId() + 1)
		{
			mSuperBlock = superBlockTwo;

			Log.d("using super block 1");
		}
		else
		{
			throw new DatabaseException("Database appears to be corrupt. SuperBlock versions are illegal: " + superBlockOne.getTransactionId() + " / " + superBlockTwo.getTransactionId());
		}

		Log.dec();
	}


	private void writeSuperBlock()
	{
		mSuperBlock.incrementTransactionId();

		long pageIndex = mSuperBlock.getTransactionId() & 1L;

		Log.i("write super block %d", pageIndex);
		Log.inc();

		mSuperBlock.write(mBlockDevice, pageIndex);

		Log.dec();
	}


	@Override
	public String toString()
	{
		return getSpaceMap();
	}


	/**
	 * Return the space map layout as a String (ranges of free blocks). If the space map is fragmented this may be a long String.
	 */
	public String getSpaceMap()
	{
		return mSpaceMap.getRangeMap().toString();
	}
	public RangeMap getRangeMap()
	{
		return mSpaceMap.getRangeMap();
	}


	/**
	 * Return the maximum available space this block device can theoretically allocate. This value may be greater than what the underlying block device can support.
	 */
	@Override
	public long getMaximumSpace()
	{
		return mSpaceMap.getRangeMap().getFreeSpace();
	}


	/**
	 * Return the size of the underlying block device, ie. size of a file acting as a block storage.
	 */
	@Override
	public long getAllocatedSpace()
	{
		return mBlockDevice.length();
	}


	/**
	 * Return the number of free blocks within the allocated space.
	 */
	@Override
	public long getFreeSpace()
	{
		return mBlockDevice.length() - mSpaceMap.getRangeMap().getUsedSpace();
	}


	/**
	 * Return the number of blocks actually used.
	 */
	@Override
	public long getUsedSpace()
	{
		return mSpaceMap.getRangeMap().getUsedSpace();
	}


	@Override
	public void setLength(long aNewLength)
	{
		mBlockDevice.setLength(aNewLength + RESERVED_BLOCKS);
	}


	@Override
	public void clear()
	{
		mBlockDevice.setLength(0);

		mSpaceMap.reset();

		createBlockDevice();
	}


	public int roundUp(int aSize)
	{
		int s = mBlockDevice.getBlockSize();
		return aSize + ((s - (aSize % s)) % s);
	}
}

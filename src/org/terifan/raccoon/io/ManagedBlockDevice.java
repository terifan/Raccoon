package org.terifan.raccoon.io;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import org.terifan.raccoon.security.ISAAC;
import org.terifan.raccoon.serialization.FieldCategory;
import org.terifan.raccoon.serialization.Marshaller;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;


/**
 * SuperBlock layout:
 *   16 checksum
 *    8 version counter
 *    8 date/time
 *    4 extra data length
 *    4 space map block index
 *    4 space map block count
 *    4 space map length
 *    - extra data
 */
public class ManagedBlockDevice implements IManagedBlockDevice, AutoCloseable
{
	private final static int RESERVED_BLOCKS = 2;
	private final static int CHECKSUM_SIZE = 16;

//	private final static int SUPERBLOCK_OFFSET_VERSION              = 16;
//	private final static int SUPERBLOCK_OFFSET_DATETIME             = 16+8;
//	private final static int SUPERBLOCK_OFFSET_SPACEMAP_BLOCK_INDEX = 16+8+8;
//	private final static int SUPERBLOCK_OFFSET_SPACEMAP_BLOCK_COUNT = 16+8+8+4;
//	private final static int SUPERBLOCK_OFFSET_SPACEMAP_LENGTH      = 16+8+8+4+4;
//	private final static int SUPERBLOCK_OFFSET_SPACEMAP_BLOCK_KEY   = 16+8+8+4+4+4;
//	private final static int SUPERBLOCK_OFFSET_EXTRA_DATA_LENGTH    = 16+8+8+4+4+4+8;
//	private final static int SUPERBLOCK_HEADER_SIZE                 = 16+8+8+4+4+4+8+4; // 56

	private IPhysicalBlockDevice mBlockDevice;
	private RangeMap mRangeMap;
	private RangeMap mPendingRangeMap;
	private boolean mModified;
	private boolean mWasCreated;
	private SuperBlock mSuperBlock;
	private int mBlockSize;
	private HashSet<Integer> mUncommitedAllocations;
//	private int mSpaceMapBlockIndex;
//	private int mSpaceMapBlockCount;
//	private int mSpaceMapLength;
//	private long mSpaceMapBlockKey;
	private boolean mDoubleCommit;


	public ManagedBlockDevice(IPhysicalBlockDevice aBlockDevice) throws IOException
	{
		if (aBlockDevice.getBlockSize() < 512)
		{
			throw new IllegalArgumentException("Block device must have 512 byte block size or larger.");
		}

		mBlockDevice = aBlockDevice;
		mBlockSize = aBlockDevice.getBlockSize();
		mWasCreated = mBlockDevice.length() == 0;
		mUncommitedAllocations = new HashSet<>();
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

		mRangeMap = new RangeMap();
		mRangeMap.add(0, Integer.MAX_VALUE);

		mPendingRangeMap = new RangeMap();
		mPendingRangeMap.add(0, Integer.MAX_VALUE);

//		mSuperBlockVersion = -1;
		mSuperBlock = new SuperBlock();
		mSuperBlock.mCreated = new Date();

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
		readSpaceMap();

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
		return mBlockDevice.length();
	}


	@Override
	public void close() throws IOException
	{
		rollback();
	}


	@Override
	public int getBlockSize()
	{
		return mBlockSize;
	}


	@Override
	public synchronized long allocBlock(int aBlockCount) throws IOException
	{
		long blockIndex = allocBlockInternal(aBlockCount) - RESERVED_BLOCKS;

		if (blockIndex < 0)
		{
			throw new IOException("Illegal block index allocated.");
		}

		return blockIndex;
	}


	private long allocBlockInternal(int aBlockCount) throws IOException
	{
		int blockIndex = mRangeMap.next(aBlockCount);

		if (blockIndex == -1)
		{
			return -1;
		}

		Log.v("alloc block " + blockIndex + " +" + aBlockCount);

		mModified = true;

		for (int i = 0; i < aBlockCount; i++)
		{
			mUncommitedAllocations.add(blockIndex + i);
		}

		mPendingRangeMap.remove(blockIndex, aBlockCount);

		return blockIndex;
	}


	@Override
	public synchronized void freeBlock(long aBlockIndex, int aBlockCount) throws IOException
	{
		if (aBlockIndex < 0)
		{
			throw new IOException("Illegal offset: " + aBlockIndex);
		}

		freeBlockInternal(RESERVED_BLOCKS + aBlockIndex, aBlockCount);
	}


	private void freeBlockInternal(long aBlockIndex, int aBlockCount) throws IOException
	{
		Log.v("free block " + aBlockIndex + " +" + aBlockCount);

		mModified = true;

		int blockIndex = (int)aBlockIndex;

		for (int i = 0; i < aBlockCount; i++)
		{
			Integer bi = blockIndex + i;

			if (mUncommitedAllocations.remove(bi))
			{
				mRangeMap.add(bi, 1);
			}
		}

		mPendingRangeMap.add(blockIndex, aBlockCount);
	}


	@Override
	public synchronized void writeBlock(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, long aBlockKey) throws IOException
	{
		if (aBlockIndex < 0)
		{
			throw new IOException("Illegal offset: " + aBlockIndex);
		}
		if ((aBufferLength % mBlockSize) != 0)
		{
			throw new IOException("Illegal buffer length: " + aBlockIndex);
		}

		writeBlockInternal(RESERVED_BLOCKS + aBlockIndex, aBuffer, aBufferOffset, aBufferLength, aBlockKey);
	}


	private void writeBlockInternal(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, long aBlockKey) throws IOException
	{
		if (!mRangeMap.isFree((int)aBlockIndex, aBufferLength / mBlockSize))
		{
			throw new IOException("Range not allocted: " + aBlockIndex + " +" + (aBufferLength / mBlockSize));
		}

		Log.v("write block " + aBlockIndex + " +" + aBufferLength/mBlockSize);
		Log.inc();

		mModified = true;

		mBlockDevice.writeBlock(aBlockIndex, aBuffer, aBufferOffset, aBufferLength, aBlockKey);

		Log.dec();
	}


	@Override
	public synchronized void readBlock(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, long aBlockKey) throws IOException
	{
		if (aBlockIndex < 0)
		{
			throw new IOException("Illegal offset: " + aBlockIndex);
		}
		if ((aBufferLength % mBlockSize) != 0)
		{
			throw new IOException("Illegal buffer length: " + aBlockIndex);
		}

		readBlockInternal(aBlockIndex + RESERVED_BLOCKS, aBuffer, aBufferOffset, aBufferLength, aBlockKey);
	}


	private void readBlockInternal(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, long aBlockKey) throws IOException
	{
		if (!mRangeMap.isFree((int)aBlockIndex, aBufferLength / mBlockSize))
		{
			throw new IOException("Range not allocted: " + aBlockIndex + " +" + (aBufferLength / mBlockSize));
		}

		Log.v("read block " + aBlockIndex + " +" + aBufferLength/mBlockSize);
		Log.inc();

		mBlockDevice.readBlock(aBlockIndex, aBuffer, aBufferOffset, aBufferLength, aBlockKey);

		Log.dec();
	}


	@Override
	public synchronized void commit(boolean aMetadata) throws IOException
	{
		commit();
	}


	@Override
	public synchronized void commit() throws IOException
	{
		if (mModified)
		{
			Log.i("committing managed block device");
			Log.inc();

			writeSpaceMap();

			if (mDoubleCommit)
			{
				mBlockDevice.commit(false);
			}

			writeSuperBlock();

			mBlockDevice.commit(true);

			mUncommitedAllocations.clear();
			mRangeMap = mPendingRangeMap.clone();
			mWasCreated = false;
			mModified = false;

			Log.dec();
		}
	}


	@Override
	public synchronized void rollback() throws IOException
	{
		if (mModified)
		{
			Log.i("rollbacking block device");
			Log.inc();

			mUncommitedAllocations.clear();

			mPendingRangeMap = mRangeMap.clone();

			init();

			mModified = false;

			Log.dec();
		}
	}


	private void readSuperBlock() throws IOException
	{
		Log.v("read super block");
		Log.inc();

		SuperBlock superBlockOne = new SuperBlock();
		SuperBlock superBlockTwo = new SuperBlock();

		superBlockOne.unmarshal(0L);
		superBlockTwo.unmarshal(1L);

		if (superBlockOne.mIndex == superBlockTwo.mIndex + 1)
		{
//			mSuperBlockVersion = versionOne;
			mSuperBlock = superBlockOne;
		}
		else if (superBlockTwo.mIndex == superBlockOne.mIndex + 1)
		{
//			mSuperBlockVersion = versionTwo;
			mSuperBlock = superBlockTwo;
		}
		else
		{
			throw new IOException("SuperBlock versions are illegal: " + superBlockOne.mIndex + " / " + superBlockTwo.mIndex);
		}

//		mSpaceMapBlockIndex = ByteArray.getInt(mSuperBlock, SUPERBLOCK_OFFSET_SPACEMAP_BLOCK_INDEX);
//		mSpaceMapBlockCount = ByteArray.getInt(mSuperBlock, SUPERBLOCK_OFFSET_SPACEMAP_BLOCK_COUNT);
//		mSpaceMapLength = ByteArray.getInt(mSuperBlock, SUPERBLOCK_OFFSET_SPACEMAP_LENGTH);
//		mSpaceMapBlockKey = ByteArray.getLong(mSuperBlock, SUPERBLOCK_OFFSET_SPACEMAP_BLOCK_KEY);

		Log.dec();
	}


	private synchronized void writeSuperBlock() throws IOException
	{
		Log.v("write super block");
		Log.inc();

		mSuperBlock.mIndex++;
		mSuperBlock.mUpdated = new Date();

//		ByteArray.putLong(mSuperBlock, SUPERBLOCK_OFFSET_VERSION, mSuperBlockVersion);
//		ByteArray.putLong(mSuperBlock, SUPERBLOCK_OFFSET_DATETIME, System.currentTimeMillis());
//		ByteArray.putInt(mSuperBlock, SUPERBLOCK_OFFSET_SPACEMAP_BLOCK_INDEX, mSpaceMapBlockIndex);
//		ByteArray.putInt(mSuperBlock, SUPERBLOCK_OFFSET_SPACEMAP_BLOCK_COUNT, mSpaceMapBlockCount);
//		ByteArray.putInt(mSuperBlock, SUPERBLOCK_OFFSET_SPACEMAP_LENGTH, mSpaceMapLength);
//		ByteArray.putLong(mSuperBlock, SUPERBLOCK_OFFSET_SPACEMAP_BLOCK_KEY, mSpaceMapBlockKey);

		ByteArrayBuffer buffer = new ByteArrayBuffer(new byte[mBlockSize]);
		buffer.position(CHECKSUM_SIZE); // leave space for checksum

		Marshaller m = new Marshaller(SuperBlock.class);
		ByteArrayBuffer buf = m.marshal(buffer, mSuperBlock, FieldCategory.VALUE);

		long pageIndex = mSuperBlock.mIndex & 1L;

		writeCheckedBlock(pageIndex, buf, -pageIndex);

		Log.dec();
	}


	private void readSpaceMap() throws IOException
	{
		Log.v("read space map " + mSuperBlock.mSpaceMapBlockIndex + " +" + mSuperBlock.mSpaceMapBlockCount + " (bytes used " + mSuperBlock.mSpaceMapLength + ")");
		Log.inc();

		mRangeMap = new RangeMap();

		if (mSuperBlock.mSpaceMapBlockCount == 0) // all blocks are free in this device
		{
			mRangeMap.add(0, Integer.MAX_VALUE);
		}
		else
		{
			if (mSuperBlock.mSpaceMapBlockIndex < 0)
			{
				throw new IOException("Block at illegal offset: " + mSuperBlock.mSpaceMapBlockIndex);
			}

			ByteArrayBuffer buffer = readCheckedBlock(mSuperBlock.mSpaceMapBlockIndex, mSuperBlock.mSpaceMapBlockKey, mSuperBlock.mSpaceMapBlockCount * mBlockSize);

			buffer.position(16);
			buffer.limit(mSuperBlock.mSpaceMapLength);

			mRangeMap.read(buffer);

			mRangeMap.remove(mSuperBlock.mSpaceMapBlockIndex, mSuperBlock.mSpaceMapBlockCount);
		}

		mPendingRangeMap = mRangeMap.clone();

		Log.dec();
	}


	private void writeSpaceMap() throws IOException
	{
		Log.v("write space map");
		Log.inc();

		if (mSuperBlock.mSpaceMapBlockCount > 0)
		{
			freeBlockInternal(mSuperBlock.mSpaceMapBlockIndex, mSuperBlock.mSpaceMapBlockCount);
		}

		ByteArrayBuffer buffer = new ByteArrayBuffer(mBlockSize);
		buffer.position(CHECKSUM_SIZE); // leave space for checksum

		mPendingRangeMap.write(buffer);

		// Allocate space for the new space map block
		mSuperBlock.mSpaceMapBlockCount = (buffer.position() + mBlockSize - 1) / mBlockSize;
		mSuperBlock.mSpaceMapBlockIndex = (int)allocBlockInternal(mSuperBlock.mSpaceMapBlockCount);
		mSuperBlock.mSpaceMapLength = buffer.position();
		mSuperBlock.mSpaceMapBlockKey = ISAAC.PRNG.nextLong();

		// Pad buffer to block size
		buffer.trim(mBlockSize * mSuperBlock.mSpaceMapBlockCount);

		writeCheckedBlock(mSuperBlock.mSpaceMapBlockIndex, buffer, mSuperBlock.mSpaceMapBlockKey);

		Log.dec();
	}


	private MessageDigest getMessageDigest() throws IOException
	{
		try
		{
			return MessageDigest.getInstance("MD5");
		}
		catch (NoSuchAlgorithmException e)
		{
			throw new IOException(e);
		}
	}


	private ByteArrayBuffer readCheckedBlock(long aBlockIndex, long aBlockKey, int aLength) throws IOException
	{
		ByteArrayBuffer buffer = new ByteArrayBuffer(aLength);

		mBlockDevice.readBlock(aBlockIndex, buffer.array(), 0, aLength, aBlockKey);

		verifyChecksum(aBlockIndex, buffer);

		assert buffer.position() == CHECKSUM_SIZE;

		return buffer;
	}


	private void verifyChecksum(long aBlockIndex, ByteArrayBuffer aBuffer) throws IOException
	{
		MessageDigest messageDigest = getMessageDigest();
		messageDigest.update((byte)aBlockIndex);
		messageDigest.update(aBuffer.array(), CHECKSUM_SIZE, aBuffer.capacity() - CHECKSUM_SIZE);

		byte[] actualDigest = messageDigest.digest();
		byte[] expectedDigest = aBuffer.position(0).read(new byte[CHECKSUM_SIZE]);

		if (!Arrays.equals(expectedDigest, actualDigest))
		{
			throw new IOException("Checksum error at block index " + aBlockIndex);
		}
	}


	private void writeCheckedBlock(long aBlockIndex, ByteArrayBuffer aBuffer, long aBlockKey) throws IOException
	{
		if (aBlockIndex < 0)
		{
			throw new IOException("Block at illegal offset: " + aBlockIndex);
		}

		MessageDigest messageDigest = getMessageDigest();
		messageDigest.update((byte)aBlockIndex);
		messageDigest.update(aBuffer.array(), CHECKSUM_SIZE, aBuffer.capacity() - CHECKSUM_SIZE);
		byte[] digest = messageDigest.digest();

		aBuffer.position(0).write(digest);

		mBlockDevice.writeBlock(aBlockIndex, aBuffer.array(), 0, aBuffer.capacity(), aBlockKey);
	}


	@Override
	public void setExtraData(byte[] aExtraData)
	{
		Log.v("set extra data");

		if (aExtraData != null && aExtraData.length > EXTRA_DATA_LIMIT)
		{
			throw new IllegalArgumentException("Length of extra data exceeds maximum length: extra length: " + aExtraData.length + ", limit: " + EXTRA_DATA_LIMIT);
		}

		mModified = true;

		mSuperBlock.mExtraData = aExtraData == null ? null : aExtraData.clone();

//		if (aExtraData == null)
//		{
//			aExtraData = new byte[0];
//		}
//		if (aExtraData.length > mBlockSize - SUPERBLOCK_HEADER_SIZE)
//		{
//			throw new IllegalArgumentException("Length of extra data exceeds maximum length: extra length: " + aExtraData.length + ", limit: " + (mBlockSize - SUPERBLOCK_HEADER_SIZE));
//		}
//
//		ByteArray.putInt(mSuperBlock, SUPERBLOCK_OFFSET_EXTRA_DATA_LENGTH, aExtraData.length);
//		System.arraycopy(aExtraData, 0, mSuperBlock, SUPERBLOCK_HEADER_SIZE, aExtraData.length);

		// padd remainder of block
//		for (int i = SUPERBLOCK_HEADER_SIZE + aExtraData.length, j = 0; i < mBlockSize; i++, j++)
//		{
//			mSuperBlock[i] = (byte)j;
//		}
	}


	@Override
	public byte[] getExtraData()
	{
//		int length = ByteArray.getInt(mSuperBlock, SUPERBLOCK_OFFSET_EXTRA_DATA_LENGTH);
//		return Arrays.copyOfRange(mSuperBlock, SUPERBLOCK_HEADER_SIZE, SUPERBLOCK_HEADER_SIZE + length);
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


	class SuperBlock
	{
		long mIndex;
		Date mCreated;
		Date mUpdated;
		int mSpaceMapBlockIndex;
		int mSpaceMapBlockCount;
		int mSpaceMapLength;
		long mSpaceMapBlockKey;
		byte[] mExtraData;


		void unmarshal(long aPageIndex) throws IOException
		{
			assert aPageIndex == 0 || aPageIndex == 1;

			ByteArrayBuffer buffer = readCheckedBlock(aPageIndex, -aPageIndex, mBlockSize);

			Marshaller m = new Marshaller(SuperBlock.class);
			m.unmarshal(buffer, this, FieldCategory.VALUE);
		}
	}
}

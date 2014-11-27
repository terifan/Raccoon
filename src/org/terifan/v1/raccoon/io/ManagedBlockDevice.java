package org.terifan.v1.raccoon.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashSet;
import org.terifan.v1.io.ByteArray;
import org.terifan.v1.security.ISAAC;
import org.terifan.v1.util.log.Log;


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
public class ManagedBlockDevice implements IBlockDevice, AutoCloseable
{
	private final static boolean VERBOSE = false;

	private final static int CHECKSUM_SIZE = 16;
	private final static int SUPERBLOCK_OFFSET_VERSION              = 16;
	private final static int SUPERBLOCK_OFFSET_DATETIME             = 16+8;
	private final static int SUPERBLOCK_OFFSET_SPACEMAP_BLOCK_INDEX = 16+8+8;
	private final static int SUPERBLOCK_OFFSET_SPACEMAP_BLOCK_COUNT = 16+8+8+4;
	private final static int SUPERBLOCK_OFFSET_SPACEMAP_LENGTH      = 16+8+8+4+4;
	private final static int SUPERBLOCK_OFFSET_SPACEMAP_BLOCK_KEY   = 16+8+8+4+4+4;
	private final static int SUPERBLOCK_OFFSET_EXTRA_DATA_LENGTH    = 16+8+8+4+4+4+8;
	private final static int SUPERBLOCK_HEADER_SIZE                 = 16+8+8+4+4+4+8+4;

	private IPhysicalBlockDevice mBlockDevice;
	private RangeMap mRangeMap;
	private RangeMap mPendingRangeMap;
	private int mBlockSize;
	private long mSuperBlockVersion;
	private boolean mModified;
	private boolean mWasCreated;
	private byte[] mSuperBlock;
	private int mSpaceMapBlockIndex;
	private int mSpaceMapBlockCount;
	private int mSpaceMapLength;
	private HashSet<Integer> mUncommitedAllocations;
	private long mSpaceMapBlockKey;

//	private HashMap<Long,LazyBlock> mLazyBlocks = new HashMap<>();


	public ManagedBlockDevice(IPhysicalBlockDevice aBlockDevice) throws IOException
	{
		mBlockDevice = aBlockDevice;
		mBlockSize = aBlockDevice.getBlockSize();
		mWasCreated = mBlockDevice.length() == 0;
		mUncommitedAllocations = new HashSet<>();

		init();
	}


	private void init() throws IOException
	{
		if (VERBOSE) Log.out.println("ManagedBlockDevice  init " + mWasCreated);

		if (mWasCreated)
		{
			mRangeMap = new RangeMap();
			mRangeMap.add(0, Integer.MAX_VALUE);

			mPendingRangeMap = new RangeMap();
			mPendingRangeMap.add(0, Integer.MAX_VALUE);

			mSuperBlockVersion = -1;
			mSuperBlock = new byte[mBlockSize];

			setExtraData(null);

			long index = allocBlock(1);
			if (index != 0)
			{
				throw new IllegalStateException("Expected block 0 but was " + index);
			}

			index = allocBlock(1);
			if (index != 1)
			{
				throw new IllegalStateException("Expected block 1 but was " + index);
			}

			// write both copies of super block
			writeSuperBlock();
			writeSuperBlock();
		}
		else
		{
			readSuperBlock();
			readSpaceMap();
		}
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
		int blockIndex = mRangeMap.next(aBlockCount);

		if (blockIndex == -1)
		{
			return -1;
		}

		if (VERBOSE) Log.out.println("ManagedBlockDevice  allocBlock " + blockIndex + " +" + aBlockCount);

		mModified = true;

		for (int i = 0; i < aBlockCount; i++)
		{
			mUncommitedAllocations.add(blockIndex + i);
		}

		mPendingRangeMap.remove(blockIndex, aBlockCount);

		return blockIndex;
	}


//	@Override
//	public long allocBlock(Result<Integer> aBlockCount) throws IOException
//	{
//		int blockIndex = mRangeMap.next(aBlockCount);
//
//		if (blockIndex == -1)
//		{
//			return -1;
//		}
//
//		if (VERBOSE) Log.out.println("ManagedBlockDevice  allocBlock " + blockIndex + " +" + aBlockCount);
//
//		mModified = true;
//
//		for (int i = 0; i < aBlockCount.get(); i++)
//		{
//			mUncommitedAllocations.add(blockIndex + i);
//		}
//
//		mPendingRangeMap.remove(blockIndex, aBlockCount.get());
//
//		return blockIndex;
//	}


	@Override
	public synchronized void freeBlock(long aBlockIndex, int aBlockCount) throws IOException
	{
		if (VERBOSE) Log.out.println("ManagedBlockDevice  freeBlock " + aBlockIndex + " +" + aBlockCount);

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
		if (!mRangeMap.isFree((int)aBlockIndex, aBufferLength / mBlockSize))
		{
			throw new IOException("Range not allocted: " + aBlockIndex + " +" + (aBufferLength / mBlockSize));
		}

		if (VERBOSE) Log.out.println("ManagedBlockDevice  writeBlock " + aBlockIndex + " +" + aBufferLength/mBlockSize);

		mModified = true;

		mBlockDevice.writeBlock(aBlockIndex, aBuffer, aBufferOffset, aBufferLength, aBlockKey);
	}


	@Override
	public synchronized void readBlock(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, long aBlockKey) throws IOException
	{
		if (!mRangeMap.isFree((int)aBlockIndex, aBufferLength / mBlockSize))
		{
			throw new IOException("Range not allocted: " + aBlockIndex + " +" + (aBufferLength / mBlockSize));
		}

		if (VERBOSE) Log.out.println("ManagedBlockDevice  readBlock " + aBlockIndex + " +" + aBufferLength/mBlockSize);

		mBlockDevice.readBlock(aBlockIndex, aBuffer, aBufferOffset, aBufferLength, aBlockKey);
	}


	@Override
	public synchronized void commit() throws IOException
	{
		if (mModified)
		{
			if (VERBOSE) Log.out.println("ManagedBlockDevice  commit");

			writeSpaceMap();

			mBlockDevice.commit(false);

			writeSuperBlock();

			mBlockDevice.commit(true);

			mUncommitedAllocations.clear();
			mRangeMap = mPendingRangeMap.clone();
			mWasCreated = false;
			mModified = false;
		}
	}


	@Override
	public synchronized void rollback() throws IOException
	{
		if (mModified)
		{
			if (VERBOSE) Log.out.println("ManagedBlockDevice  rollback");

			mUncommitedAllocations.clear();

			mPendingRangeMap = mRangeMap.clone();

			init();

			mModified = false;
		}
	}


	private void readSuperBlock() throws IOException
	{
		if (VERBOSE) Log.out.println("ManagedBlockDevice  readSuperBlock");

		byte[] bufferOne = new byte[mBlockSize];
		byte[] bufferTwo = new byte[mBlockSize];

		readCheckedBlock(0L, bufferOne, 0L);
		readCheckedBlock(1L, bufferTwo, 0L);

		long versionOne = ByteArray.BE.getLong(bufferOne, SUPERBLOCK_OFFSET_VERSION);
		long versionTwo = ByteArray.BE.getLong(bufferTwo, SUPERBLOCK_OFFSET_VERSION);

		if (versionOne == versionTwo + 1)
		{
			mSuperBlockVersion = versionOne;
			mSuperBlock = bufferOne;
		}
		else if (versionTwo == versionOne + 1)
		{
			mSuperBlockVersion = versionTwo;
			mSuperBlock = bufferTwo;
		}
		else
		{
			throw new IOException("SuperBlock versions are illegal: " + versionOne + " / " + versionTwo);
		}

		mSpaceMapBlockIndex = ByteArray.BE.getInt(mSuperBlock, SUPERBLOCK_OFFSET_SPACEMAP_BLOCK_INDEX);
		mSpaceMapBlockCount = ByteArray.BE.getInt(mSuperBlock, SUPERBLOCK_OFFSET_SPACEMAP_BLOCK_COUNT);
		mSpaceMapLength = ByteArray.BE.getInt(mSuperBlock, SUPERBLOCK_OFFSET_SPACEMAP_LENGTH);
		mSpaceMapBlockKey = ByteArray.BE.getLong(mSuperBlock, SUPERBLOCK_OFFSET_SPACEMAP_BLOCK_KEY);
	}


	private synchronized void writeSuperBlock() throws IOException
	{
		if (VERBOSE) Log.out.println("ManagedBlockDevice  writeSuperBlock");

		mSuperBlockVersion++;

		ByteArray.BE.putLong(mSuperBlock, SUPERBLOCK_OFFSET_VERSION, mSuperBlockVersion);
		ByteArray.BE.putLong(mSuperBlock, SUPERBLOCK_OFFSET_DATETIME, System.currentTimeMillis());
		ByteArray.BE.putInt(mSuperBlock, SUPERBLOCK_OFFSET_SPACEMAP_BLOCK_INDEX, mSpaceMapBlockIndex);
		ByteArray.BE.putInt(mSuperBlock, SUPERBLOCK_OFFSET_SPACEMAP_BLOCK_COUNT, mSpaceMapBlockCount);
		ByteArray.BE.putInt(mSuperBlock, SUPERBLOCK_OFFSET_SPACEMAP_LENGTH, mSpaceMapLength);
		ByteArray.BE.putLong(mSuperBlock, SUPERBLOCK_OFFSET_SPACEMAP_BLOCK_KEY, mSpaceMapBlockKey);

		writeCheckedBlock(mSuperBlockVersion & 1L, mSuperBlock, 0L);
	}


	private void readSpaceMap() throws IOException
	{
		if (VERBOSE) Log.out.println("ManagedBlockDevice  readSpaceMap " + mSpaceMapBlockIndex + " +" + mSpaceMapBlockCount + " (" + mSpaceMapLength + ")");

		mRangeMap = new RangeMap();

		if (mSpaceMapBlockCount > 0)
		{
			byte[] buffer = new byte[mSpaceMapBlockCount * mBlockSize];

			readCheckedBlock(mSpaceMapBlockIndex, buffer, mSpaceMapBlockKey);

//			ByteBuffer bb = ByteBuffer.wrap(buffer);
//			bb.position(16);
//			bb.limit(mSpaceMapLength);

			mRangeMap.read(new ByteArrayInputStream(buffer, 16, mSpaceMapLength - 16));

			mRangeMap.remove(mSpaceMapBlockIndex, mSpaceMapBlockCount);
		}
		else
		{
			mRangeMap.add(0, Integer.MAX_VALUE);
		}

		mPendingRangeMap = mRangeMap.clone();
	}


	private void writeSpaceMap() throws IOException
	{
		if (VERBOSE) Log.out.println("ManagedBlockDevice  writeSpaceMap");

		if (mSpaceMapBlockCount > 0)
		{
			freeBlock(mSpaceMapBlockIndex, mSpaceMapBlockCount);
		}

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		baos.write(new byte[CHECKSUM_SIZE]); // allocate space for checksum

		mPendingRangeMap.write(baos);

		// Allocate space for the new space map block
		mSpaceMapBlockCount = (baos.size() + mBlockSize - 1) / mBlockSize;
		mSpaceMapBlockIndex = (int)allocBlock(mSpaceMapBlockCount);
		mSpaceMapLength = baos.size();
		mSpaceMapBlockKey = ISAAC.PRNG.nextLong();

		// Pad buffer to block size
		baos.write(new byte[mBlockSize * mSpaceMapBlockCount - mSpaceMapLength]);

		writeCheckedBlock(mSpaceMapBlockIndex, baos.toByteArray(), mSpaceMapBlockKey);
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


	private void readCheckedBlock(long aBlockIndex, byte[] aBuffer, long aBlockKey) throws IOException
	{
		if (aBlockIndex < 0)
		{
			throw new IOException("Block at illegal offset: " + aBlockIndex);
		}

		mBlockDevice.readBlock(aBlockIndex, aBuffer, 0, aBuffer.length, aBlockKey);

		MessageDigest messageDigest = getMessageDigest();
		messageDigest.update((byte)aBlockIndex);
		messageDigest.update(aBuffer, CHECKSUM_SIZE, aBuffer.length - CHECKSUM_SIZE);

		if (!ByteArray.equals(aBuffer, 0, messageDigest.digest(), 0, CHECKSUM_SIZE))
		{
			throw new IOException("Checksum error at block index " + aBlockIndex);
		}
	}


	private void writeCheckedBlock(long aBlockIndex, byte[] aBuffer, long aBlockKey) throws IOException
	{
		if (aBlockIndex < 0)
		{
			throw new IOException("Block at illegal offset: " + aBlockIndex);
		}

		try
		{
			MessageDigest messageDigest = getMessageDigest();
			messageDigest.update((byte)aBlockIndex);
			messageDigest.update(aBuffer, CHECKSUM_SIZE, aBuffer.length - CHECKSUM_SIZE);
			messageDigest.digest(aBuffer, 0, CHECKSUM_SIZE);

			mBlockDevice.writeBlock(aBlockIndex, aBuffer, 0, aBuffer.length, aBlockKey);
		}
		catch (DigestException e)
		{
			throw new IOException(e);
		}
	}


	public void setExtraData(byte[] aExtraData)
	{
		if (VERBOSE) Log.out.println("ManagedBlockDevice  setExtraData");

		mModified = true;

		if (aExtraData == null)
		{
			aExtraData = new byte[0];
		}
		if (aExtraData.length > mBlockSize - SUPERBLOCK_HEADER_SIZE)
		{
			throw new IllegalArgumentException("Length of extra data exceeds maximum length: extra length: " + aExtraData.length + ", limit: " + (mBlockSize - SUPERBLOCK_HEADER_SIZE));
		}

		ByteArray.BE.putInt(mSuperBlock, SUPERBLOCK_OFFSET_EXTRA_DATA_LENGTH, aExtraData.length);
		System.arraycopy(aExtraData, 0, mSuperBlock, SUPERBLOCK_HEADER_SIZE, aExtraData.length);

		// padd remainder of block
		for (int i = SUPERBLOCK_HEADER_SIZE + aExtraData.length, j = 0; i < mBlockSize; i++, j++)
		{
			mSuperBlock[i] = 0;
//			mSuperBlock[i] = (byte)j;
		}
	}


	public byte[] getExtraData()
	{
		int length = ByteArray.BE.getInt(mSuperBlock, SUPERBLOCK_OFFSET_EXTRA_DATA_LENGTH);
		return Arrays.copyOfRange(mSuperBlock, SUPERBLOCK_HEADER_SIZE, SUPERBLOCK_HEADER_SIZE + length);
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


//	private static class LazyBlock
//	{
//		long blockKey;
//		long blockIndex;
//		byte[] content;
//
//
//		public LazyBlock(long aBlockKey, long aBlockIndex, byte[] aContent)
//		{
//			this.blockKey = aBlockKey;
//			this.blockIndex = aBlockIndex;
//			this.content = aContent;
//		}
//	}
}

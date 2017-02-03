package org.terifan.raccoon.io;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import org.terifan.raccoon.DatabaseException;
import org.terifan.raccoon.Key;
import org.terifan.raccoon.serialization.Marshaller;
import org.terifan.raccoon.serialization.EntityDescriptor;
import org.terifan.security.random.ISAAC;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;


public class ManagedBlockDevice implements IManagedBlockDevice, AutoCloseable
{
	private final static byte FORMAT_VERSION = 1;
	private final static int CHECKSUM_SIZE = 16;
	private final static int RESERVED_BLOCKS = 2;

	private IPhysicalBlockDevice mBlockDevice;
	private RangeMap mRangeMap;
	private RangeMap mPendingRangeMap;
	private SuperBlock mSuperBlock;
	private HashSet<Integer> mUncommitedAllocations;
	private Marshaller mSuperBlockMarshaller;
	private String mBlockDeviceLabel;
	private int mBlockSize;
	private boolean mModified;
	private boolean mWasCreated;
	private boolean mDoubleCommit;
	private CachingBlockDevice mCachingBlockDevice;


	/**
	 * Create/open a ManagedBlockDevice with an empty label.
	 */
	public ManagedBlockDevice(IPhysicalBlockDevice aBlockDevice) throws IOException
	{
		this(aBlockDevice, "");
	}


	/**
	 * Create/open a ManagedBlockDevice with an user defined label.
	 *
	 * @param aBlockDeviceLabel
	 *   a label describing contents of the block device. If a non-null value is provided then this value must match the value found inside
	 *   the block device opened or an exception is thrown.
	 */
	public ManagedBlockDevice(IPhysicalBlockDevice aBlockDevice, String aBlockDeviceLabel) throws IOException
	{
		if (aBlockDevice.getBlockSize() < 512)
		{
			throw new IllegalArgumentException("Block device must have 512 byte block size or larger.");
		}

		mBlockDevice = aBlockDevice;
		mBlockDeviceLabel = aBlockDeviceLabel;
		mBlockSize = aBlockDevice.getBlockSize();
		mWasCreated = mBlockDevice.length() < ManagedBlockDevice.RESERVED_BLOCKS;
		mUncommitedAllocations = new HashSet<>();
		mDoubleCommit = true;
		mCachingBlockDevice = new CachingBlockDevice(mBlockDevice);

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

		mSuperBlock = new SuperBlock();
		mSuperBlock.mCreated = new Date();
		mSuperBlock.mWriteCounter = -1L; // counter is incremented in writeSuperBlock method and we want to ensure we write block 0 before block 1
		mSuperBlock.mFormatVersion = FORMAT_VERSION;
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
		return mBlockDevice.length() - RESERVED_BLOCKS;
	}


	@Override
	public void close() throws IOException
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
	public void forceClose() throws IOException
	{
		mCachingBlockDevice.clear();

		mUncommitedAllocations.clear();

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


	private long allocBlockInternal(int aBlockCount) throws IOException
	{
		int blockIndex = mRangeMap.next(aBlockCount);

		if (blockIndex < 0)
		{
			return -1;
		}

		Log.v("alloc block %d +%d", blockIndex, aBlockCount);

		mModified = true;

		for (int i = 0; i < aBlockCount; i++)
		{
			mUncommitedAllocations.add(blockIndex + i);
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


	private void freeBlockInternal(long aBlockIndex, int aBlockCount) throws IOException
	{
		Log.v("free block %d +%d", aBlockIndex, aBlockCount);

		mModified = true;

		mCachingBlockDevice.free(aBlockIndex);

		int blockIndex = (int)aBlockIndex;

		for (int i = 0; i < aBlockCount; i++)
		{
			if (mUncommitedAllocations.remove(blockIndex + i))
			{
				mRangeMap.add(blockIndex + i, 1);
			}
		}

		mPendingRangeMap.add(blockIndex, aBlockCount);
	}


	@Override
	public void writeBlock(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, long aBlockKey) throws IOException
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
		assert aBufferLength > 0;
		assert (aBufferLength % mBlockSize) == 0;

		if (!mRangeMap.isFree((int)aBlockIndex, aBufferLength / mBlockSize))
		{
			throw new IOException("Range not allocted: " + aBlockIndex + " +" + (aBufferLength / mBlockSize));
		}

		Log.v("write block %d +%d", aBlockIndex, aBufferLength / mBlockSize);
		Log.inc();

		mModified = true;

		mCachingBlockDevice.writeBlock(aBlockIndex, aBuffer, aBufferOffset, aBufferLength, aBlockKey);

		Log.dec();
	}


	@Override
	public void readBlock(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, long aBlockKey) throws IOException
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
		assert aBufferLength > 0;
		assert (aBufferLength % mBlockSize) == 0;

		if (!mRangeMap.isFree((int)aBlockIndex, aBufferLength / mBlockSize))
		{
			throw new IOException("Range not allocted: " + aBlockIndex + " +" + (aBufferLength / mBlockSize));
		}

		Log.v("read block %d +%d", aBlockIndex, aBufferLength/mBlockSize);
		Log.inc();

		mCachingBlockDevice.readBlock(aBlockIndex, aBuffer, aBufferOffset, aBufferLength, aBlockKey);

		Log.dec();
	}


	@Override
	public void commit(boolean aMetadata) throws IOException
	{
		if (mModified)
		{
			// TODO: non raccoon implementations might not need this
//			if (!mSuperBlock.mExtraDataModified)
//			{
//				throw new IOException("ExtraData not modified!");
//			}
			
			mCachingBlockDevice.flush();

			Log.i("committing managed block device");
			Log.inc();

			writeSpaceMap();

			if (mDoubleCommit) // enabled by default
			{
				// commit twice since write operations on disk may occur out of order ie. superblock may be written before spacemap even
				// tough calls made in reverse order
				mBlockDevice.commit(false);
			}

			writeSuperBlock();

			mBlockDevice.commit(aMetadata);

			mUncommitedAllocations.clear();
			mRangeMap = mPendingRangeMap.clone();
			mWasCreated = false;
			mModified = false;
			mSuperBlock.mExtraDataModified = false;

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

			mCachingBlockDevice.clear();

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

		mSuperBlockMarshaller = Marshaller.getInstance(EntityDescriptor.getInstance(SuperBlock.class));

		SuperBlock superBlockOne = new SuperBlock();
		SuperBlock superBlockTwo = new SuperBlock();

		try
		{
			superBlockOne.unmarshal(0L);
			superBlockTwo.unmarshal(1L);
		}
		catch (IOException e)
		{
			throw new UnsupportedVersionException("Invalid or corrupt data or data may be encrypted.", e);
		}

		if (superBlockOne.mFormatVersion != FORMAT_VERSION)
		{
			throw new UnsupportedVersionException("Data format is not supported: was " + superBlockOne.mFormatVersion + ", expected " + FORMAT_VERSION);
		}
		if (!(mBlockDeviceLabel == null || mBlockDeviceLabel.isEmpty() && superBlockOne.mBlockDeviceLabel.isEmpty() || mBlockDeviceLabel.equals(superBlockOne.mBlockDeviceLabel)))
		{
			throw new UnsupportedVersionException("Block device label don't match: was " + (superBlockOne.mBlockDeviceLabel.isEmpty()?"<empty>" : superBlockOne.mBlockDeviceLabel) + ", expected " + (mBlockDeviceLabel.isEmpty()?"<empty>" : mBlockDeviceLabel));
		}

		if (superBlockOne.mWriteCounter == superBlockTwo.mWriteCounter + 1)
		{
			mSuperBlock = superBlockOne;

			Log.v("using super block 0");
		}
		else if (superBlockTwo.mWriteCounter == superBlockOne.mWriteCounter + 1)
		{
			mSuperBlock = superBlockTwo;

			Log.v("using super block 1");
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
		mSuperBlock.mUpdated = new Date();

		Log.i("write super block %d", mSuperBlock.mWriteCounter & 1L);
		Log.inc();

		ByteArrayBuffer buffer = new ByteArrayBuffer(new byte[mBlockSize]);
		buffer.position(CHECKSUM_SIZE); // leave space for checksum

		Marshaller m = Marshaller.getInstance(EntityDescriptor.getInstance(SuperBlock.class));
		ByteArrayBuffer buf = m.marshalValues(buffer, mSuperBlock);

		if (mBlockDevice instanceof SecureBlockDevice)
		{
			byte[] padding = new byte[buf.capacity() - buf.position()];
			ISAAC.PRNG.nextBytes(padding);
			buf.write(padding);
		}

		long pageIndex = mSuperBlock.mWriteCounter & 1L;

		writeCheckedBlock(pageIndex, buf, -pageIndex);

		Log.dec();
	}


	private void readSpaceMap() throws IOException
	{
		Log.v("read space map %d +%d (bytes used %d)", mSuperBlock.mSpaceMapBlockIndex, mSuperBlock.mSpaceMapBlockCount, mSuperBlock.mSpaceMapLength);
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

			mRangeMap.unmarshal(buffer);

			mRangeMap.remove((int)mSuperBlock.mSpaceMapBlockIndex, mSuperBlock.mSpaceMapBlockCount);
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

		mPendingRangeMap.marshal(buffer);

		// Allocate space for the new space map block
		mSuperBlock.mSpaceMapBlockCount = (buffer.position() + mBlockSize - 1) / mBlockSize;
		mSuperBlock.mSpaceMapBlockIndex = allocBlockInternal(mSuperBlock.mSpaceMapBlockCount);
		mSuperBlock.mSpaceMapLength = buffer.position();
		mSuperBlock.mSpaceMapBlockKey = ISAAC.PRNG.nextLong();

		// Pad buffer to block size
		buffer.capacity(mBlockSize * mSuperBlock.mSpaceMapBlockCount);

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
		byte[] expectedDigest = new byte[CHECKSUM_SIZE];
		aBuffer.position(0).read(expectedDigest);

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
		Log.i("set extra data");

		if (aExtraData != null && aExtraData.length > EXTRA_DATA_LIMIT)
		{
			throw new IllegalArgumentException("Length of extra data exceeds maximum length: extra length: " + aExtraData.length + ", limit: " + EXTRA_DATA_LIMIT);
		}

		mModified = true;

		mSuperBlock.mExtraData = aExtraData == null ? null : aExtraData.clone();

		mSuperBlock.mExtraDataModified = true;
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

		mUncommitedAllocations.clear();

		createBlockDevice();
	}


	class SuperBlock
	{
		@Key long mDummy; // a Key field is needed to entity serialization and is never used
		byte mFormatVersion;
		Date mCreated;
		Date mUpdated;
		long mWriteCounter;
		long mSpaceMapBlockIndex;
		int mSpaceMapBlockCount;
		int mSpaceMapLength;
		long mSpaceMapBlockKey;
		String mBlockDeviceLabel;
		byte[] mExtraData;

		transient boolean mExtraDataModified;


		void unmarshal(long aPageIndex) throws IOException
		{
			assert aPageIndex == 0 || aPageIndex == 1;

			ByteArrayBuffer buffer = readCheckedBlock(aPageIndex, -aPageIndex, mBlockSize);

			mSuperBlockMarshaller.unmarshalValues(buffer, this);
		}
	}
}

package org.terifan.raccoon.io.managed;

import org.terifan.bundle.Document;
import org.terifan.raccoon.io.DatabaseIOException;
import org.terifan.raccoon.io.physical.IPhysicalBlockDevice;
import org.terifan.raccoon.io.secure.SecureBlockDevice;
import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.security.cryptography.ISAAC;
import org.terifan.security.messagedigest.MurmurHash3;


class SuperBlock
{
	private final static byte FORMAT_VERSION = 1;
	private final static int CHECKSUM_SIZE = 16;
	private final static int IV_SIZE = 16;

	private int mFormatVersion;
	private long mCreateTime;
	private long mModifiedTime;
	private long mTransactionId;
	private BlockPointer mSpaceMapPointer;
	private Document mApplicationHeader;


	public SuperBlock()
	{
		mFormatVersion = FORMAT_VERSION;
		mCreateTime = System.currentTimeMillis();
		mSpaceMapPointer = new BlockPointer();
		mTransactionId = -1L;
		mApplicationHeader = new Document();
	}


	public SuperBlock(long aTransactionId)
	{
		this();

		mTransactionId = aTransactionId;
	}


	public SuperBlock(IPhysicalBlockDevice aBlockDevice, long aBlockIndex)
	{
		this();

		read(aBlockDevice, aBlockIndex);
	}


	public BlockPointer getSpaceMapPointer()
	{
		return mSpaceMapPointer;
	}


	public long getTransactionId()
	{
		return mTransactionId;
	}


	public void incrementTransactionId()
	{
		mTransactionId++;
	}


	public int getFormatVersion()
	{
		return mFormatVersion;
	}


	public long getCreateTime()
	{
		return mCreateTime;
	}


	public void setCreateTime(long aCreateTime)
	{
		mCreateTime = aCreateTime;
	}


	public long getModifiedTime()
	{
		return mModifiedTime;
	}


	public void setModifiedTime(long aModifiedTime)
	{
		mModifiedTime = aModifiedTime;
	}


	public Document getApplicationHeader()
	{
		return mApplicationHeader;
	}


	public void read(IPhysicalBlockDevice aBlockDevice, long aBlockIndex)
	{
		int blockSize = aBlockDevice.getBlockSize();

		ByteArrayBuffer buffer = ByteArrayBuffer.alloc(blockSize, true);

		if (aBlockDevice instanceof SecureBlockDevice)
		{
			((SecureBlockDevice)aBlockDevice).readBlockWithIV(aBlockIndex, buffer.array(), 0, blockSize);
		}
		else
		{
			aBlockDevice.readBlock(aBlockIndex, buffer.array(), 0, buffer.capacity(), new long[2]);
		}

		long[] hash = MurmurHash3.hash256(buffer.array(), CHECKSUM_SIZE, blockSize - CHECKSUM_SIZE - IV_SIZE, aBlockIndex);

		buffer.position(0);

		if (buffer.readInt64() != hash[0] || buffer.readInt64() != hash[1])
		{
			throw new DatabaseIOException("Checksum error at block index " + aBlockIndex);
		}

		unmarshal(buffer);
	}


	public void write(IPhysicalBlockDevice aBlockDevice, long aBlockIndex)
	{
		if (aBlockIndex < 0)
		{
			throw new DatabaseIOException("Block at illegal offset: " + aBlockIndex);
		}

		mModifiedTime = System.currentTimeMillis();

		int blockSize = aBlockDevice.getBlockSize();

		ByteArrayBuffer buffer = ByteArrayBuffer.alloc(blockSize, true);
		buffer.position(CHECKSUM_SIZE); // reserve space for checksum

		marshal(buffer);

		if (buffer.remaining() < IV_SIZE)
		{
			throw new DatabaseIOException("SuperBlock marshalled into a too large buffer");
		}

		if (aBlockDevice instanceof SecureBlockDevice)
		{
			ISAAC.PRNG.nextBytes(buffer.array(), buffer.position(), buffer.remaining() - IV_SIZE);
		}

		long[] hash = MurmurHash3.hash256(buffer.array(), CHECKSUM_SIZE, blockSize - CHECKSUM_SIZE - IV_SIZE, aBlockIndex);

		buffer.position(0);
		buffer.writeInt64(hash[0]);
		buffer.writeInt64(hash[1]);

		if (aBlockDevice instanceof SecureBlockDevice)
		{
			((SecureBlockDevice)aBlockDevice).writeBlockWithIV(aBlockIndex, buffer.array(), 0, blockSize);
		}
		else
		{
			aBlockDevice.writeBlock(aBlockIndex, buffer.array(), 0, blockSize, new long[2]);
		}
	}


	private void marshal(ByteArrayBuffer aBuffer)
	{
		aBuffer.writeInt8(mFormatVersion);
		aBuffer.writeInt64(mCreateTime);
		aBuffer.writeInt64(mModifiedTime);
		aBuffer.writeInt64(mTransactionId);
		mSpaceMapPointer.marshal(aBuffer);
		byte[] x = mApplicationHeader.marshal();
		aBuffer.writeVar32(x.length);
		aBuffer.write(x);
	}


	private void unmarshal(ByteArrayBuffer aBuffer)
	{
		mFormatVersion = aBuffer.readInt8();

		if (mFormatVersion != FORMAT_VERSION)
		{
			throw new UnsupportedVersionException("Data format is not supported: was " + mFormatVersion + ", expected " + FORMAT_VERSION);
		}

		mCreateTime = aBuffer.readInt64();
		mModifiedTime = aBuffer.readInt64();
		mTransactionId = aBuffer.readInt64();
		mSpaceMapPointer.unmarshal(aBuffer);
		mApplicationHeader = Document.unmarshal(aBuffer.read(new byte[aBuffer.readVar32()]));
	}
}

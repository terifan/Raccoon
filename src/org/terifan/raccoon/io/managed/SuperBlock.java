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
	private final static int CHECKSUM_SIZE = 32;
	private final static int IV_SIZE = 16; // same as in SecureBlockDevice
	private final static int HEADER_SIZE = 1 + 3 * 8 + 2;
	private final static int TOTAL_OVERHEAD = IV_SIZE + CHECKSUM_SIZE + HEADER_SIZE + BlockPointer.SIZE;
	private final static ISAAC PRNG = new ISAAC();

	private int mFormatVersion;
	private long mCreateTime;
	private long mModifiedTime;
	private long mTransactionId;
	private BlockPointer mSpaceMapPointer;
	private Document mApplicationMetadata;


	public SuperBlock(long aTransactionId)
	{
		mFormatVersion = FORMAT_VERSION;
		mCreateTime = System.currentTimeMillis();
		mSpaceMapPointer = new BlockPointer();
		mApplicationMetadata = new Document();
		mTransactionId = aTransactionId;
	}


	public SuperBlock(IPhysicalBlockDevice aBlockDevice, long aBlockIndex, long aTransactionId)
	{
		this(aTransactionId);

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


	public Document getApplicationMetadata()
	{
		return mApplicationMetadata;
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

		if (buffer.readInt64() != hash[0] || buffer.readInt64() != hash[1] || buffer.readInt64() != hash[2] || buffer.readInt64() != hash[3])
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
		byte[] metadata = mApplicationMetadata.marshal();

		if (TOTAL_OVERHEAD + metadata.length > blockSize)
		{
			throw new DatabaseIOException("Application metadata exeeds maximum size: limit: " + (blockSize - TOTAL_OVERHEAD) + ", metadata: " + metadata.length);
		}

		ByteArrayBuffer buffer = ByteArrayBuffer.alloc(blockSize, true);
		buffer.position(CHECKSUM_SIZE); // reserve space for checksum

		marshal(buffer, metadata);

		if (aBlockDevice instanceof SecureBlockDevice)
		{
			PRNG.nextBytes(buffer.array(), buffer.position(), buffer.remaining() - IV_SIZE);
		}

		long[] hash = MurmurHash3.hash256(buffer.array(), CHECKSUM_SIZE, blockSize - CHECKSUM_SIZE - IV_SIZE, aBlockIndex);

		buffer.position(0);
		buffer.writeInt64(hash[0]);
		buffer.writeInt64(hash[1]);
		buffer.writeInt64(hash[2]);
		buffer.writeInt64(hash[3]);

		if (aBlockDevice instanceof SecureBlockDevice)
		{
			((SecureBlockDevice)aBlockDevice).writeBlockWithIV(aBlockIndex, buffer.array(), 0, blockSize);
		}
		else
		{
			aBlockDevice.writeBlock(aBlockIndex, buffer.array(), 0, blockSize, new long[2]);
		}
	}


	private void marshal(ByteArrayBuffer aBuffer, byte[] aApplicationHeader)
	{
		aBuffer.writeInt8(mFormatVersion);
		aBuffer.writeInt64(mCreateTime);
		aBuffer.writeInt64(mModifiedTime);
		aBuffer.writeInt64(mTransactionId);
		mSpaceMapPointer.marshal(aBuffer);
		aBuffer.writeInt16(aApplicationHeader.length);
		aBuffer.write(aApplicationHeader);
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
		mApplicationMetadata = Document.unmarshal(aBuffer.read(new byte[aBuffer.readInt16()]));
	}
}

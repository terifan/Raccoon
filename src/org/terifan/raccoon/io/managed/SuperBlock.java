package org.terifan.raccoon.io.managed;

import java.io.IOException;
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
	public static final int DEVICE_HEADER_LABEL_MAX_LENGTH = 32;

	private int mFormatVersion;
	private long mCreateTime;
	private long mModifiedTime;
	private long mTransactionId;
	private DeviceHeader mTenantHeader;
	private DeviceHeader mApplicationHeader;
	private BlockPointer mSpaceMapPointer;
	private byte[] mApplicationPointer;


	public SuperBlock()
	{
		mFormatVersion = FORMAT_VERSION;
		mCreateTime = System.currentTimeMillis();
		mSpaceMapPointer = new BlockPointer();
		mTransactionId = -1L;

		mApplicationHeader = new DeviceHeader();
		mTenantHeader = new DeviceHeader();
		mApplicationPointer = new byte[0];
	}


	public SuperBlock(long aTransactionId)
	{
		this();

		mTransactionId = aTransactionId;
	}


	public SuperBlock(IPhysicalBlockDevice aBlockDevice, long aBlockIndex) throws IOException
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


	DeviceHeader getApplicationHeader()
	{
		return mApplicationHeader;
	}


	void setApplicationHeader(DeviceHeader aApplicationHeader)
	{
		mApplicationHeader = aApplicationHeader;
	}


	DeviceHeader getTenantHeader()
	{
		return mTenantHeader;
	}


	void setTenantHeader(DeviceHeader aTenantHeader)
	{
		mTenantHeader = aTenantHeader;
	}


	public byte[] getApplicationPointer()
	{
		return mApplicationPointer;
	}


	public void setApplicationPointer(byte[] aApplicationPointer)
	{
		mApplicationPointer = aApplicationPointer;
	}


	public void read(IPhysicalBlockDevice aBlockDevice, long aBlockIndex) throws IOException
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

		long[] hash = MurmurHash3.hash128(buffer.array(), CHECKSUM_SIZE, blockSize - CHECKSUM_SIZE - IV_SIZE, aBlockIndex);

		buffer.position(0);

		if (buffer.readInt64() != hash[0] || buffer.readInt64() != hash[1])
		{
			throw new IOException("Checksum error at block index " + aBlockIndex);
		}

		unmarshal(buffer);
	}


	public void write(IPhysicalBlockDevice aBlockDevice, long aBlockIndex) throws IOException
	{
		if (aBlockIndex < 0)
		{
			throw new IOException("Block at illegal offset: " + aBlockIndex);
		}

		mModifiedTime = System.currentTimeMillis();

		int blockSize = aBlockDevice.getBlockSize();

		ByteArrayBuffer buffer = ByteArrayBuffer.alloc(blockSize, true);
		buffer.position(CHECKSUM_SIZE); // reserve space for checksum

		marshal(buffer);

		if (buffer.remaining() < IV_SIZE)
		{
			throw new IOException("SuperBlock marshalled into a too large buffer");
		}

		if (aBlockDevice instanceof SecureBlockDevice)
		{
			ISAAC.PRNG.nextBytes(buffer.array(), buffer.position(), buffer.remaining() - IV_SIZE);
		}

		long[] hash = MurmurHash3.hash128(buffer.array(), CHECKSUM_SIZE, blockSize - CHECKSUM_SIZE - IV_SIZE, aBlockIndex);

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


	private void marshal(ByteArrayBuffer aBuffer) throws IOException
	{
		if (mApplicationPointer == null)
		{
			throw new IllegalStateException("The application pointer must be specified");
		}
		if (mApplicationPointer.length > IManagedBlockDevice.APPLICATION_POINTER_MAX_SIZE)
		{
			throw new IllegalStateException("The application pointer is too long");
		}

		aBuffer.writeInt8(mFormatVersion);
		aBuffer.writeInt64(mCreateTime);
		aBuffer.writeInt64(mModifiedTime);
		aBuffer.writeInt64(mTransactionId);
		mSpaceMapPointer.marshal(aBuffer);
		aBuffer.writeInt8(mApplicationPointer.length);
		aBuffer.write(mApplicationPointer);
		mApplicationHeader.marshal(aBuffer);
		mTenantHeader.marshal(aBuffer);
	}


	private void unmarshal(ByteArrayBuffer aBuffer) throws IOException
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
		mApplicationPointer = aBuffer.read(new byte[aBuffer.readInt8()]);
		mApplicationHeader.unmarshal(aBuffer);
		mTenantHeader.unmarshal(aBuffer);
	}
}

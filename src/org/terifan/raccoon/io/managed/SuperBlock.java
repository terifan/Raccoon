package org.terifan.raccoon.io.managed;

import java.io.IOException;
import org.terifan.raccoon.io.physical.IPhysicalBlockDevice;
import org.terifan.raccoon.io.secure.SecureBlockDevice;
import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.security.cryptography.ISAAC;
import org.terifan.security.messagedigest.MurmurHash3;


public class SuperBlock
{
	private final static byte FORMAT_VERSION = 1;
	private final static int CHECKSUM_SIZE = 16;
	private final static int IV_SIZE = 16;
	private final static int MAX_LABEL_LENGTH = 32;

	private int mFormatVersion;
	private long mCreateTime;
	private long mModifiedTime;
	private long mTransactionId;
	private String mBlockDeviceLabel;
	private byte[] mApplicationHeader;
	private BlockPointer mSpaceMapPointer;
	private byte[] mInstanceId;
	private byte[] mApplicationId;
	private int mApplicationVersion;


	public SuperBlock()
	{
		mFormatVersion = FORMAT_VERSION;
		mCreateTime = System.currentTimeMillis();
		mSpaceMapPointer = new BlockPointer();
		mTransactionId = -1L;
		mApplicationId = new byte[16];
		mInstanceId = new byte[16];
		mApplicationHeader = new byte[0];

		ISAAC.PRNG.nextBytes(mInstanceId);
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


	public String getBlockDeviceLabel()
	{
		return mBlockDeviceLabel;
	}


	public void setBlockDeviceLabel(String aBlockDeviceLabel)
	{
		mBlockDeviceLabel = aBlockDeviceLabel;
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


	public byte[] getApplicationHeader()
	{
		return mApplicationHeader.clone();
	}


	public void setApplicationHeader(byte[] aApplicationHeader)
	{
		if (aApplicationHeader.length > IManagedBlockDevice.EXTRA_DATA_LIMIT)
		{
			throw new IllegalArgumentException("Application header is to long");
		}

		mApplicationHeader = aApplicationHeader.clone();
	}


	public byte[] getInstanceId()
	{
		return mInstanceId.clone();
	}


	public byte[] getApplicationId()
	{
		return mApplicationId.clone();
	}


	public void setApplicationId(byte[] aApplicationId)
	{
		if (aApplicationId == null || aApplicationId.length != 16)
		{
			throw new IllegalArgumentException("Instance ID must be 16 bytes in length");
		}

		mApplicationId = aApplicationId.clone();
	}


	public int getApplicationVersion()
	{
		return mApplicationVersion;
	}


	public void setApplicationVersion(int aApplicationVersion)
	{
		mApplicationVersion = aApplicationVersion;
	}


	public void read(IPhysicalBlockDevice aBlockDevice, long aBlockIndex) throws IOException
	{
		int blockSize = aBlockDevice.getBlockSize();

		ByteArrayBuffer buffer = new ByteArrayBuffer(blockSize);

		if (aBlockDevice instanceof SecureBlockDevice)
		{
			((SecureBlockDevice)aBlockDevice).readBlockWithIV(aBlockIndex, buffer.array(), 0, blockSize);
		}
		else
		{
			aBlockDevice.readBlock(aBlockIndex, buffer.array(), 0, buffer.capacity(), new long[2]);
		}

		long[] hash = MurmurHash3.hash_x64_128(buffer.array(), CHECKSUM_SIZE, blockSize - CHECKSUM_SIZE - IV_SIZE, aBlockIndex);

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

		ByteArrayBuffer buffer = new ByteArrayBuffer(blockSize);
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

		long[] hash = MurmurHash3.hash_x64_128(buffer.array(), CHECKSUM_SIZE, blockSize - CHECKSUM_SIZE - IV_SIZE, aBlockIndex);

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
		byte[] label = mBlockDeviceLabel == null ? new byte[0] : mBlockDeviceLabel.getBytes("utf-8");

		aBuffer.writeInt8(mFormatVersion);
		aBuffer.writeInt64(mCreateTime);
		aBuffer.writeInt64(mModifiedTime);
		aBuffer.writeInt64(mTransactionId);
		mSpaceMapPointer.marshal(aBuffer);
		aBuffer.writeInt8(label.length);
		aBuffer.write(label);
		aBuffer.write(mInstanceId);
		aBuffer.write(mApplicationId);
		aBuffer.writeInt32(mApplicationVersion);
		aBuffer.writeInt16(mApplicationHeader.length);
		aBuffer.write(mApplicationHeader);
	}


	private void unmarshal(ByteArrayBuffer aBuffer) throws IOException
	{
		mFormatVersion = aBuffer.readInt8();
		mCreateTime = aBuffer.readInt64();
		mModifiedTime = aBuffer.readInt64();
		mTransactionId = aBuffer.readInt64();
		mSpaceMapPointer.unmarshal(aBuffer);
		mBlockDeviceLabel = aBuffer.readString(aBuffer.readInt8());
		aBuffer.read(mInstanceId);
		aBuffer.read(mApplicationId);
		mApplicationVersion = aBuffer.readInt32();
		mApplicationHeader = aBuffer.read(new byte[aBuffer.readInt16()]);

		if (mFormatVersion != FORMAT_VERSION)
		{
			throw new UnsupportedVersionException("Data format is not supported: was " + mFormatVersion + ", expected " + FORMAT_VERSION);
		}
	}
}

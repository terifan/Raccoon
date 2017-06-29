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
	private final static int NULL_CODE = 65535;
	private final static int CHECKSUM_SIZE = 16;
	private final static int IV_SIZE = 16;

	private int mFormatVersion;
	private long mCreated;
	long mUpdated;
	long mWriteCounter;
	String mBlockDeviceLabel;
	byte[] mExtraData;
	BlockPointer mSpaceMapPointer;


	public SuperBlock()
	{
		mFormatVersion = FORMAT_VERSION;
		mCreated = System.currentTimeMillis();
		mSpaceMapPointer = new BlockPointer();
	}


	public SuperBlock(IPhysicalBlockDevice aBlockDevice, long aBlockIndex) throws IOException
	{
		this();

		read(aBlockDevice, aBlockIndex);
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

		mUpdated = System.currentTimeMillis();

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
		aBuffer.writeInt8(mFormatVersion);
		aBuffer.writeInt64(mCreated);
		aBuffer.writeInt64(mUpdated);
		aBuffer.writeInt64(mWriteCounter);
		mSpaceMapPointer.marshal(aBuffer);
		aBuffer.writeInt8(mBlockDeviceLabel.length());
		aBuffer.writeString(mBlockDeviceLabel);
		if (mExtraData == null)
		{
			aBuffer.writeInt16(NULL_CODE);
		}
		else if (mExtraData.length >= NULL_CODE)
		{
			throw new IllegalStateException("Illegal length: " + mExtraData.length);
		}
		else
		{
			aBuffer.writeInt16(mExtraData.length);
			aBuffer.write(mExtraData);
		}
	}


	private void unmarshal(ByteArrayBuffer aBuffer) throws IOException
	{
		mFormatVersion = aBuffer.readInt8();
		mCreated = aBuffer.readInt64();
		mUpdated = aBuffer.readInt64();
		mWriteCounter = aBuffer.readInt64();
		mSpaceMapPointer.unmarshal(aBuffer);
		mBlockDeviceLabel = aBuffer.readString(aBuffer.readInt8());
		int len = aBuffer.readInt16();
		if (len == NULL_CODE)
		{
			mExtraData = null;
		}
		else
		{
			mExtraData = aBuffer.read(new byte[len]);
		}

		if (mFormatVersion != FORMAT_VERSION)
		{
			throw new UnsupportedVersionException("Data format is not supported: was " + mFormatVersion + ", expected " + FORMAT_VERSION);
		}
	}
}

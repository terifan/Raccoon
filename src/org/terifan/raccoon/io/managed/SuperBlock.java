package org.terifan.raccoon.io.managed;

import java.io.IOException;
import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.ByteArrayBuffer;


/**
 *   8 format version
 *  64 created
 *  64 last updated
 *  64 write counter
 * 512 space map block pointer
 *   8 label length
 *   n label
 *  16 extra length
 *   n extra
 */
class SuperBlock
{
	private final static byte FORMAT_VERSION = 1;
	private final static int NULL_CODE = 65535;

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


	void marshal(ByteArrayBuffer aBuffer) throws IOException
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


	void unmarshal(ByteArrayBuffer aBuffer) throws IOException
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

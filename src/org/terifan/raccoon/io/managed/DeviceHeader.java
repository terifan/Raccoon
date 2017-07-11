package org.terifan.raccoon.io.managed;

import java.io.UnsupportedEncodingException;
import java.util.UUID;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.security.cryptography.ISAAC;


public class DeviceHeader
{
	private byte[] mLabel;
	private int mMajorVersion;
	private int mMinorVersion;
	private byte[] mSerialNumber;


	DeviceHeader()
	{
		this("");
	}


	public DeviceHeader(String aLabel)
	{
		this(aLabel, 0, 0, UUID.randomUUID());
	}


	public DeviceHeader(String aLabel, int aMajorVersion, int aMinorVersion, UUID aSerialNumber)
	{
		if (aMajorVersion < 0 || aMajorVersion > 255)
		{
			throw new IllegalArgumentException("Illegal major version, must be 0-255: " + aMajorVersion);
		}
		if (aMinorVersion < 0 || aMinorVersion > 255)
		{
			throw new IllegalArgumentException("Illegal minor version, must be 0-255: " + aMinorVersion);
		}

		try
		{
			mLabel = aLabel.getBytes("utf-8");

			if (mLabel.length > SuperBlock.DEVICE_HEADER_LABEL_MAX_LENGTH)
			{
				throw new IllegalArgumentException("The block device label exceed maximum length.");
			}

			mMajorVersion = aMajorVersion;
			mMinorVersion = aMinorVersion;
			mSerialNumber = new byte[16];
			ByteArrayBuffer.writeInt64(mSerialNumber, 0, aSerialNumber.getMostSignificantBits());
			ByteArrayBuffer.writeInt64(mSerialNumber, 0, aSerialNumber.getLeastSignificantBits());

			ISAAC.PRNG.nextBytes(mSerialNumber);
		}
		catch (UnsupportedEncodingException e)
		{
			throw new IllegalArgumentException(e);
		}
	}


	public int getMajorVersion()
	{
		return mMajorVersion;
	}


	public int getMinorVersion()
	{
		return mMinorVersion;
	}


	public String getLabel()
	{
		try
		{
			return new String(mLabel, "utf-8");
		}
		catch (UnsupportedEncodingException e)
		{
			throw new IllegalArgumentException(e);
		}
	}


	public byte[] getSerialNumberBytes()
	{
		return mSerialNumber.clone();
	}


	public UUID getSerialNumberUUID()
	{
		return new UUID(ByteArrayBuffer.readInt64(mSerialNumber, 0), ByteArrayBuffer.readInt64(mSerialNumber, 8));
	}


	void setLabel(byte[] aLabel)
	{
		mLabel = aLabel;
	}
 	
	
	public void marshal(ByteArrayBuffer aBuffer)
	{
		aBuffer.writeInt8(mMajorVersion);
		aBuffer.writeInt8(mMinorVersion);
		aBuffer.writeInt8(mLabel.length);
		aBuffer.write(mLabel);
		aBuffer.write(mSerialNumber);
	}
 	
	
	public void unmarshal(ByteArrayBuffer aBuffer)
	{
		mMajorVersion = aBuffer.readInt8();
		mMinorVersion = aBuffer.readInt8();
		mLabel = new byte[aBuffer.readInt8()];
		aBuffer.read(mLabel);
		aBuffer.read(mSerialNumber);
	}


	@Override
	public String toString()
	{
		return getLabel() + "[" + mMajorVersion + "." + mMinorVersion + "]";
	}
}

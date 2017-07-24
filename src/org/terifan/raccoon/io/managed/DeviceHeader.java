package org.terifan.raccoon.io.managed;

import java.io.UnsupportedEncodingException;
import java.util.UUID;
import org.terifan.raccoon.OpenParam;
import org.terifan.raccoon.util.ByteArrayBuffer;


public class DeviceHeader implements OpenParam
{
	private byte[] mLabel;
	private int mMajorVersion;
	private int mMinorVersion;
	private final byte[] mSerialNumber;


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

		setLabel(aLabel);

		mMajorVersion = aMajorVersion;
		mMinorVersion = aMinorVersion;
		mSerialNumber = new byte[16];
		ByteArrayBuffer.writeInt64(mSerialNumber, 0, aSerialNumber.getMostSignificantBits());
		ByteArrayBuffer.writeInt64(mSerialNumber, 8, aSerialNumber.getLeastSignificantBits());
	}


	public int getMajorVersion()
	{
		return mMajorVersion;
	}


	public void setMajorVersion(int aMajorVersion)
	{
		mMajorVersion = aMajorVersion;
	}


	public int getMinorVersion()
	{
		return mMinorVersion;
	}


	public void setMinorVersion(int aMinorVersion)
	{
		mMinorVersion = aMinorVersion;
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


	public void setLabel(String aLabel)
	{
		try
		{
			mLabel = aLabel.getBytes("utf-8");
		}
		catch (UnsupportedEncodingException e)
		{
			throw new IllegalArgumentException(e);
		}

		if (mLabel.length > SuperBlock.DEVICE_HEADER_LABEL_MAX_LENGTH)
		{
			throw new IllegalArgumentException("The label exceeds maximum length.");
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
		return "{label=" + getLabel() + ", version=" + mMajorVersion + "." + mMinorVersion + ", serial=" + getSerialNumberUUID() + "}";
	}
}

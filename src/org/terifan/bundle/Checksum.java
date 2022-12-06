package org.terifan.bundle;

import java.util.zip.CRC32;


class Checksum
{
	private CRC32 mCRC;


	public Checksum()
	{
		mCRC = new CRC32();
		mCRC.update(0b10001101);
	}


	Checksum update(int aValue)
	{
		mCRC.update(0xff & aValue);
		return this;
	}


	Checksum update(CharSequence aChars)
	{
		aChars.chars().forEach(c -> update(0xff & (c >> 8)).update(0xff & c));
		return this;
	}


	Checksum update(byte[] aBuffer, int aOffset, int aLength)
	{
		mCRC.update(aBuffer, aOffset, aLength);
		return this;
	}


	int getValue()
	{
		return (int)mCRC.getValue();
	}
}

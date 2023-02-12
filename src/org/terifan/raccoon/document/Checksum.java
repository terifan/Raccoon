package org.terifan.raccoon.document;

import java.util.zip.CRC32;


class Checksum
{
	private final CRC32 mCRC;


	Checksum()
	{
		mCRC = new CRC32();
		mCRC.update(0b10001101);
	}


	Checksum updateByte(int aValue)
	{
		mCRC.update(aValue);
		return this;
	}


	Checksum updateInt(int aValue)
	{
		mCRC.update(0xFF & (aValue >>> 24));
		mCRC.update(0xFF & (aValue >> 16));
		mCRC.update(0xFF & (aValue >> 8));
		mCRC.update(0xFF & aValue);
		return this;
	}


	Checksum updateChars(CharSequence aChars)
	{
		aChars.chars().forEach(c -> {
			if (c > 255)
			{
				mCRC.update(0xff & (c >> 8));
			}
			mCRC.update(0xff & c);
		});
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

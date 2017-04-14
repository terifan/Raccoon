package org.terifan.security.cryptography.rsa;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;


class DERWriter
{
	private ByteArrayOutputStream mOutputStream;


	public DERWriter()
	{
		mOutputStream = new ByteArrayOutputStream();
	}


	public void write(BigInteger aValue) throws IOException
	{
		mOutputStream.write(DERConstants.INTEGER);

		byte[] buf = aValue.toByteArray();
		writeLength(buf.length);
		mOutputStream.write(buf);
	}


	public void write(int aValue) throws IOException
	{
		write(BigInteger.valueOf(aValue));
	}


	public void write(DERWriter aSequence) throws IOException
	{
		mOutputStream.write(DERConstants.SEQUENCE + DERConstants.CONSTRUCTED);

		byte[] buf = aSequence.toByteArray();
		writeLength(buf.length);
		mOutputStream.write(buf);
	}


	private void writeLength(int aLength) throws IOException
	{
		if (aLength < 0x80)
		{
			mOutputStream.write(aLength);
			return;
		}

		byte[] buf = BigInteger.valueOf(aLength).toByteArray();
		mOutputStream.write(0x80 + buf.length);
		mOutputStream.write(buf);
	}


	public byte[] toByteArray()
	{
		return mOutputStream.toByteArray();
	}
}

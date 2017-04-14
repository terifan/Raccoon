package org.terifan.security.cryptography.rsa;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;


class DERReader
{
	private InputStream mInputStream;


	public DERReader(byte[] bytes) throws IOException
	{
		mInputStream = new ByteArrayInputStream(bytes);
	}


	public Asn1Object read() throws IOException
	{
		int tag = mInputStream.read();
		if (tag == -1)
		{
			throw new IOException("Invalid DER: stream too short, missing tag");
		}

		byte[] value = new byte[getLength()];

		if (value.length != mInputStream.read(value))
		{
			throw new IOException("Invalid DER: stream too short, missing value");
		}

		return new Asn1Object(tag, value);
	}


	private int getLength() throws IOException
	{
		int i = mInputStream.read();
		if (i == -1)
		{
			throw new IOException("Invalid DER: length missing");
		}
		if ((i & 0x80) == 0)
		{
			return i;
		}

		int len = i & 0x7F;
		if (len > 4)
		{
			throw new IOException("Invalid DER: length field too big (" + len + ")");
		}

		byte[] bytes = new byte[len];
		if (mInputStream.read(bytes) != len)
		{
			throw new IOException("Invalid DER: length too short");
		}

		return new BigInteger(1, bytes).intValue();
	}
}

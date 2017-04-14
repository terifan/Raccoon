package org.terifan.security.cryptography.rsa;

import java.io.IOException;
import java.math.BigInteger;


class Asn1Object
{
	private final int mType;
	private final byte[] mValue;
	private final int mTag;


	public Asn1Object(int tag, byte[] value)
	{
		mTag = tag;
		mType = tag & 0x1F;
		mValue = value;
	}


	public int getType()
	{
		return mType;
	}


	public byte[] getValue()
	{
		return mValue;
	}


	public boolean isConstructed()
	{
		return (mTag & DERConstants.CONSTRUCTED) == DERConstants.CONSTRUCTED;
	}


	public DERReader getParser() throws IOException
	{
		if (!isConstructed())
		{
			throw new IOException("Invalid DER: can't parse primitive entity");
		}
		return new DERReader(mValue);
	}


	public BigInteger getInteger() throws IOException
	{
		if (mType != DERConstants.INTEGER)
		{
			throw new IOException("Invalid DER: object is not integer");
		}

		return new BigInteger(mValue);
	}


	public String getString() throws IOException
	{
		String encoding;

		switch (mType)
		{
			// Not all are Latin-1 but it's the closest thing
			case DERConstants.NUMERIC_STRING:
			case DERConstants.PRINTABLE_STRING:
			case DERConstants.VIDEOTEX_STRING:
			case DERConstants.IA5_STRING:
			case DERConstants.GRAPHIC_STRING:
			case DERConstants.ISO646_STRING:
			case DERConstants.GENERAL_STRING:
				encoding = "ISO-8859-1";
				break;
			case DERConstants.BMP_STRING:
				encoding = "UTF-16BE";
				break;
			case DERConstants.UTF8_STRING:
				encoding = "UTF-8";
				break;
			case DERConstants.UNIVERSAL_STRING:
				throw new IOException("Invalid DER: can't handle UCS-4 string");
			default:
				throw new IOException("Invalid DER: object is not a string");
		}

		return new String(mValue, encoding);
	}
}

package org.terifan.raccoon;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import org.terifan.raccoon.serialization.Marshaller;
import org.terifan.raccoon.security.MurmurHash3;
import org.terifan.raccoon.serialization.FieldCategory;


public final class TableType
{
	private Class mClass;
	private String mName;
	private byte[] mUniqueName;
	private byte[] mDiscriminatorKey;
	private Marshaller mMarshaller;
	private Object mDiscriminator;
	private final int mHash;


	public TableType(Class aClass, Object aDiscriminator)
	{
		mClass = aClass;
		mDiscriminator = aDiscriminator;
		mMarshaller = new Marshaller(mClass);
		mName = mClass.getName();

		mDiscriminatorKey = createDiscriminatorKey(aDiscriminator);

		try
		{
			mUniqueName = join(mName.getBytes("utf-8"), mDiscriminatorKey);
		}
		catch (UnsupportedEncodingException e)
		{
			throw new IllegalStateException(e);
		}

		mHash = MurmurHash3.hash_x86_32(mUniqueName, 0x94936d91);
	}


	byte[] createDiscriminatorKey(Object aDiscriminator)
	{
		if (aDiscriminator == null)
		{
			return new byte[0];
		}

		return mMarshaller.marshal(aDiscriminator, FieldCategory.DISCRIMINATOR);
	}


	public Object getDiscriminator()
	{
		return mDiscriminator;
	}


	public String getName()
	{
		return mName;
	}


	public Class getType()
	{
		return mClass;
	}


	Marshaller getMarshaller()
	{
		return mMarshaller;
	}


	@Override
	public boolean equals(Object aOther)
	{
		if (aOther instanceof TableType)
		{
			TableType other = (TableType)aOther;
			return Arrays.equals(mUniqueName, other.mUniqueName);
		}

		return false;
	}


	@Override
	public int hashCode()
	{
		return mHash;
	}


	byte[] getDiscriminatorKey()
	{
		return mDiscriminatorKey;
	}


	private static byte[] join(byte[] aBuffer0, byte[] aBuffer1)
	{
		byte[] buf = new byte[aBuffer0.length + aBuffer1.length];
		System.arraycopy(aBuffer0, 0, buf, 0, aBuffer0.length);
		System.arraycopy(aBuffer1, 0, buf, aBuffer0.length, aBuffer1.length);
		return buf;
	}
}

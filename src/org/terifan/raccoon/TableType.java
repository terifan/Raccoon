package org.terifan.raccoon;

import org.terifan.raccoon.serialization.Marshaller;
import org.terifan.raccoon.security.MurmurHash3;
import org.terifan.raccoon.serialization.FieldCategory;
import org.terifan.raccoon.util.Log;


public class TableType
{
	private Class mClass;
	private String mName;
	private Marshaller mMarshaller;
	private int mHash;
	private Object mDiscriminator;


	public TableType(Class aClass, Object aDiscriminator)
	{
		mClass = aClass;
		mDiscriminator = aDiscriminator;



		mName = mClass.getName();
		mMarshaller = new Marshaller(aClass);

		byte[] disc = mMarshaller.marshal(aDiscriminator, FieldCategory.DISCRIMINATOR);

		Log.out.println(mName);
		Log.hexDump(disc);

		mHash = MurmurHash3.hash_x86_32(mName.getBytes(), 0x94936d91) ^ MurmurHash3.hash_x86_32(disc, 0x94936d91);
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
			if ((mDiscriminator == null) != (other.mDiscriminator == null))
			{
				return false;
			}
			return other.mName.equals(mName) && (mDiscriminator == null || other.mDiscriminator.equals(mDiscriminator));
		}

		return false;
	}


	@Override
	public int hashCode()
	{
		return mHash;
	}
}

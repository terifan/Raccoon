package org.terifan.raccoon;

import org.terifan.raccoon.serialization.Marshaller;
import org.terifan.raccoon.security.MurmurHash3;


public class TableType
{
	private Class mClass;
	private String mName;
	private Marshaller mMarshaller;
	private int mHash;


	public TableType(Class aClass)
	{
		mClass = aClass;
		mName = mClass.getName();

		mHash = MurmurHash3.hash_x86_32(mName, 0x94936d91);
		mMarshaller = new Marshaller(aClass);
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
			return other.mName.equals(mName);
		}

		return false;
	}


	@Override
	public int hashCode()
	{
		return mHash;
	}
}

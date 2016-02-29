package org.terifan.raccoon.serialization;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


public class FieldType implements Comparable<FieldType>, Externalizable
{
	private final static long serialVersionUID = 1L;

	private String mName;
	private boolean mNullable;
	private boolean mArray;
	private ContentType mContentType;
	private FieldCategory mCategory;


	public FieldType()
	{
	}


	@Override
	public void writeExternal(ObjectOutput aOut) throws IOException
	{
		int flags =
			  (mNullable ? 1 : 0)
			+ (mArray ? 2 : 0);
		aOut.write(flags);

		aOut.writeUTF(mName);
		aOut.write(mContentType.ordinal());
		aOut.write(mCategory.ordinal());
	}


	@Override
	public void readExternal(ObjectInput aIn) throws IOException, ClassNotFoundException
	{
		int flags = aIn.read();
		mNullable = (flags & 1) != 0;
		mArray = (flags & 2) != 0;

		mName = aIn.readUTF();
		mContentType = ContentType.values()[aIn.read()];
		mCategory = FieldCategory.values()[aIn.read()];
	}


	@Override
	public int compareTo(FieldType aOther)
	{
		return mName.compareTo(aOther.mName);
	}


	public FieldCategory getCategory()
	{
		return mCategory;
	}


	public void setCategory(FieldCategory aCategory)
	{
		mCategory = aCategory;
	}


	public String getName()
	{
		return mName;
	}


	public void setName(String aName)
	{
		mName = aName;
	}


	public ContentType getContentType()
	{
		return mContentType;
	}


	public void setContentType(ContentType aContentType)
	{
		mContentType = aContentType;
	}


	public boolean isNullable()
	{
		return mNullable;
	}


	public void setNullable(boolean aNullable)
	{
		mNullable = aNullable;
	}


	public boolean isArray()
	{
		return mArray;
	}


	public void setArray(boolean aArray)
	{
		this.mArray = aArray;
	}


	@Override
	public String toString()
	{
		return mCategory + " " + mContentType + (mArray ? "[]" : "") + (mNullable ? " nullable " : " ") + mName;
	}
}

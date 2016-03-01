package org.terifan.raccoon.serialization;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Field;


public class FieldType implements Comparable<FieldType>, Externalizable
{
	private int mIndex;
	private String mName;
	private boolean mNullable;
	private boolean mArray;
	private ContentType mContentType;
	private FieldCategory mCategory;
	private String mDescription;
	private int mDepth;

	private transient Field mField;


	public FieldType()
	{
	}


	public Field getField()
	{
		return mField;
	}


	public void setField(Field aField)
	{
		mField = aField;
	}


	public int getIndex()
	{
		return mIndex;
	}


	public void setIndex(int aIndex)
	{
		mIndex = aIndex;
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
		mArray = aArray;
	}


	public String getDescription()
	{
		return mDescription;
	}


	public void setDescription(String aDescription)
	{
		mDescription = aDescription;
	}


	public int getDepth()
	{
		return mDepth;
	}


	public void setDepth(int aDepth)
	{
		mDepth = aDepth;
	}


	@Override
	public void writeExternal(ObjectOutput aOutput) throws IOException
	{
		int flags =
			  (mNullable ? 1 : 0)
			+ (mArray ? 2 : 0);
		aOutput.write(flags);

		aOutput.writeShort(mIndex);
		aOutput.writeUTF(mName);
		aOutput.write(mContentType.ordinal());
		aOutput.write(mCategory.ordinal());
		aOutput.writeUTF(mDescription);
		aOutput.write(mDepth);
	}


	@Override
	public void readExternal(ObjectInput aInput) throws IOException, ClassNotFoundException
	{
		int flags = aInput.read();
		mNullable = (flags & 1) != 0;
		mArray = (flags & 2) != 0;

		mIndex = aInput.readShort();
		mName = aInput.readUTF();
		mContentType = ContentType.values()[aInput.read()];
		mCategory = FieldCategory.values()[aInput.read()];
		mDescription = aInput.readUTF();
		mDepth = aInput.read();
	}


	@Override
	public int compareTo(FieldType aOther)
	{
		return mName.compareTo(aOther.mName);
	}


	@Override
	public String toString()
	{
		return mCategory + " " + mContentType + (mArray ? "[]" : "") + (mNullable ? " nullable " : " ") + mName;
	}
}

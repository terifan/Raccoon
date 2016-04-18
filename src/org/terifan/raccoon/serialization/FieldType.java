package org.terifan.raccoon.serialization;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Field;
import static org.terifan.raccoon.serialization.TypeMappings.*;


public class FieldType implements Comparable<FieldType>, Externalizable
{
	private static final long serialVersionUID = 1L;

	private int mIndex;
	private String mName;
	private boolean mNullable;
	private boolean mArray;
	private ContentType mContentType;
	private FieldCategory mCategory;
	private String mTypeName;
	private int mDepth;

	private transient Field mField;


	public FieldType()
	{
	}


	public Field getField()
	{
		if (mField == null)
		{
			throw new Error("mField not initialized: " + toString());
		}
		
		return mField;
	}


	void setField(Field aField)
	{
		mField = aField;
	}


	public int getIndex()
	{
		return mIndex;
	}


	void setIndex(int aIndex)
	{
		mIndex = aIndex;
	}


	public FieldCategory getCategory()
	{
		return mCategory;
	}


	void setCategory(FieldCategory aCategory)
	{
		mCategory = aCategory;
	}


	public String getName()
	{
		return mName;
	}


	void setName(String aName)
	{
		mName = aName;
	}


	public ContentType getContentType()
	{
		return mContentType;
	}


	void setContentType(ContentType aContentType)
	{
		mContentType = aContentType;
	}


	public boolean isNullable()
	{
		return mNullable;
	}


	void setNullable(boolean aNullable)
	{
		mNullable = aNullable;
	}


	public boolean isArray()
	{
		return mArray;
	}


	void setArray(boolean aArray)
	{
		mArray = aArray;
	}


	public String getTypeName()
	{
		return mTypeName;
	}


	void setTypeName(String aTypeName)
	{
		mTypeName = aTypeName;
	}


	public int getDepth()
	{
		return mDepth;
	}


	void setDepth(int aDepth)
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

		aOutput.writeUTF(mName);
		aOutput.writeShort(mIndex);
		aOutput.write(mContentType.ordinal());
		aOutput.writeUTF(mTypeName);
		aOutput.write(mCategory.ordinal());
		aOutput.write(mDepth);
	}


	@Override
	public void readExternal(ObjectInput aInput) throws IOException, ClassNotFoundException
	{
		int flags = aInput.read();
		mNullable = (flags & 1) != 0;
		mArray = (flags & 2) != 0;

		mName = aInput.readUTF();
		mIndex = aInput.readShort();
		mContentType = ContentType.values()[aInput.read()];
		mTypeName = aInput.readUTF();
		mCategory = FieldCategory.values()[aInput.read()];
		mDepth = aInput.read();
	}


	@Override
	public int hashCode()
	{
		return mName.hashCode() ^ mIndex ^ mContentType.ordinal() ^ mTypeName.hashCode() ^ mCategory.ordinal() ^ mDepth ^ (mArray ? 1 : 0) ^ (mNullable ? 2 : 0);
	}


	@Override
	public boolean equals(Object aOther)
	{
		if (aOther instanceof FieldType)
		{
			FieldType other = (FieldType)aOther;

			return mName.equals(other.mName)
				&& mTypeName.equals(other.mTypeName)
				&& mArray == other.mArray
				&& mDepth == other.mDepth
				&& mIndex == other.mIndex
				&& (mField == null && other.mField == null || mField != null && mField.equals(other.mField))
				&& mNullable == other.mNullable
				&& mContentType == other.mContentType
				&& mCategory == other.mCategory;
		}

		return false;
	}


	@Override
	public int compareTo(FieldType aOther)
	{
		return mName.compareTo(aOther.mName);
	}


	@Override
	public String toString()
	{
		String s = mContentType.toString().toLowerCase();
		if (mNullable)
		{
			s = s.substring(0, 1).toUpperCase() + s.substring(1);
		}
		s = s.replace("Int", "Integer");
		s = s.replace("Char", "Character");
		for (int i = 0; i < mDepth; i++)
		{
			s += "[]";
		}
		s += " " + mName;

		return s;
	}


	Class getTypeClass()
	{
		return mNullable ? TYPE_CLASSES.get(mContentType) : TYPE_VALUES.get(mContentType);
	}
}

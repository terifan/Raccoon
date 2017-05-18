package org.terifan.raccoon.serialization;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Field;
import static org.terifan.raccoon.serialization.TypeMappings.*;


public class FieldDescriptor implements Comparable<FieldDescriptor>, Externalizable
{
	private static final long serialVersionUID = 1L;

	private int mIndex;
	private int mCategory;
	private int mDepth;
	private ValueType mValueType;
	private boolean mNullable;
	private boolean mArray;
	private boolean mPrimitive;
	private String mName;
	private String mTypeName;

	private transient Field mField;


	public FieldDescriptor()
	{
	}


	public FieldDescriptor(int aIndex, int aCategory, int aDepth, ValueType aValueType, boolean aNullable, boolean aArray, boolean aPrimitive, String aName, String aTypeName, Field aField)
	{
		this.mIndex = aIndex;
		this.mCategory = aCategory;
		this.mDepth = aDepth;
		this.mValueType = aValueType;
		this.mNullable = aNullable;
		this.mArray = aArray;
		this.mPrimitive = aPrimitive;
		this.mName = aName;
		this.mTypeName = aTypeName;
		this.mField = aField;
	}


	public Field getField()
	{
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


	public int getCategory()
	{
		return mCategory;
	}


	void setCategory(int aCategory)
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


	public ValueType getValueType()
	{
		return mValueType;
	}


	void setValueType(ValueType aValueType)
	{
		mValueType = aValueType;
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


	public boolean isPrimitive()
	{
		return mPrimitive;
	}


	void setPrimitive(boolean aPrimitive)
	{
		mPrimitive = aPrimitive;
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
		int flags
			= (mNullable ? 1 : 0)
			+ (mArray ? 2 : 0)
			+ (mPrimitive ? 4 : 0);
		aOutput.write(flags);

		aOutput.writeUTF(mName);
		aOutput.writeShort(mIndex);
		aOutput.write(mValueType.ordinal());
		aOutput.writeUTF(mTypeName);
		aOutput.writeShort(mCategory);
		aOutput.write(mDepth);
	}


	@Override
	public void readExternal(ObjectInput aInput) throws IOException, ClassNotFoundException
	{
		int flags = aInput.read();
		mNullable = (flags & 1) != 0;
		mArray = (flags & 2) != 0;
		mPrimitive = (flags & 4) != 0;

		mName = aInput.readUTF();
		mIndex = aInput.readShort();
		mValueType = ValueType.values()[aInput.read()];
		mTypeName = aInput.readUTF();
		mCategory = aInput.readShort();
		mDepth = aInput.read();
	}


	@Override
	public int hashCode()
	{
		return mName.hashCode() ^ mIndex ^ mValueType.ordinal() ^ mTypeName.hashCode() ^ mCategory ^ mDepth ^ (mArray ? 1 : 0) ^ (mNullable ? 2 : 0);
	}


	@Override
	public boolean equals(Object aOther)
	{
		if (aOther instanceof FieldDescriptor)
		{
			FieldDescriptor other = (FieldDescriptor)aOther;

			return mName.equals(other.mName)
				&& mTypeName.equals(other.mTypeName)
				&& mArray == other.mArray
				&& mDepth == other.mDepth
				&& mIndex == other.mIndex
				&& (mField == null && other.mField == null || mField != null && mField.equals(other.mField))
				&& mNullable == other.mNullable
				&& mPrimitive == other.mPrimitive
				&& mValueType == other.mValueType
				&& mCategory == other.mCategory;
		}

		return false;
	}


	@Override
	public int compareTo(FieldDescriptor aOther)
	{
		return mName.compareTo(aOther.mName);
	}


	@Override
	public String toString()
	{
		String s = mValueType.toString().toLowerCase();
		if (!mPrimitive)
		{
			s = s.substring(0, 1).toUpperCase() + s.substring(1);
		}
		s = s.replace("Int", "Integer");
		s = s.replace("Char", "Character");
		StringBuilder t = new StringBuilder();
		for (int i = 0; i < mDepth; i++)
		{
			t.append("[]");
		}
		s += t + " " + mName;

		return s;
	}


	Class getTypeClass()
	{
		return mPrimitive ? TYPE_VALUES.get(mValueType) : TYPE_CLASSES.get(mValueType);
	}
}

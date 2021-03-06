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
	private boolean mLob;
	private boolean mLazy;
	private String mFieldName;
	private String mColumnName;
	private String mTypeName;

	private transient Field mField;


	public FieldDescriptor()
	{
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


	public String getFieldName()
	{
		return mFieldName;
	}


	void setFieldName(String aFieldName)
	{
		mFieldName = aFieldName;
	}


	public String getColumnName()
	{
		return mColumnName;
	}


	void setColumnName(String aColumnName)
	{
		mColumnName = aColumnName;
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


	public boolean isLob()
	{
		return mLob;
	}


	void setLob(boolean aLob)
	{
		mLob = aLob;
	}


	public boolean isLazy()
	{
		return mLazy;
	}


	void setLazy(boolean aLazy)
	{
		mLazy = aLazy;
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
			+ (mPrimitive ? 4 : 0)
			+ (mLob ? 8 : 0)
			+ (mLazy ? 16 : 0);

		aOutput.write(flags);
		aOutput.writeUTF(mFieldName);
		aOutput.writeShort(mIndex);
		aOutput.write(mValueType.ordinal());
		aOutput.writeUTF(mTypeName);
		aOutput.writeUTF(mColumnName);
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
		mLob = (flags & 8) != 0;
		mLazy = (flags & 16) != 0;

		mFieldName = aInput.readUTF();
		mIndex = aInput.readShort();
		mValueType = ValueType.values()[aInput.read()];
		mTypeName = aInput.readUTF();
		mColumnName = aInput.readUTF();
		mCategory = aInput.readShort();
		mDepth = aInput.read();
	}


	@Override
	public int hashCode()
	{
		return mColumnName.hashCode() ^ mIndex ^ mValueType.ordinal() ^ mTypeName.hashCode() ^ mCategory ^ mDepth ^ (mArray ? 1 : 0) ^ (mNullable ? 2 : 0) ^ (mPrimitive ? 4 : 0) ^ (mLob ? 8 : 0) ^ (mLazy ? 16 : 0);
	}


	@Override
	public boolean equals(Object aOther)
	{
		if (aOther instanceof FieldDescriptor)
		{
			FieldDescriptor other = (FieldDescriptor)aOther;

			return mColumnName.equals(other.mColumnName)
				&& mTypeName.equals(other.mTypeName)
				&& mArray == other.mArray
				&& mDepth == other.mDepth
				&& mIndex == other.mIndex
				&& (mField == null && other.mField == null || mField != null && mField.equals(other.mField))
				&& mNullable == other.mNullable
				&& mPrimitive == other.mPrimitive
				&& mLob == other.mLob
				&& mLazy == other.mLazy
				&& mValueType == other.mValueType
				&& mCategory == other.mCategory;
		}

		return false;
	}


	@Override
	public int compareTo(FieldDescriptor aOther)
	{
		return mFieldName.compareTo(aOther.mFieldName);
	}


	@Override
	public String toString()
	{
		return toTypeNameString();
	}


	public String toTypeNameString()
	{
		String s = mTypeName;

		s = s.replace("java.lang.Boolean", "Boolean");
		s = s.replace("java.lang.Byte", "Byte");
		s = s.replace("java.lang.Short", "Short");
		s = s.replace("java.lang.Character", "Character");
		s = s.replace("java.lang.Integer", "Integer");
		s = s.replace("java.lang.Long", "Long");
		s = s.replace("java.lang.Float", "Float");
		s = s.replace("java.lang.Double", "Double");
		s = s.replace("java.lang.String", "String");

		StringBuilder buffer = new StringBuilder();

		for (int i = 0; i < mDepth; i++)
		{
			buffer.append("[]");
		}

		s += buffer + " " + mFieldName;

		return s;
	}


	Class getTypeClass()
	{
		return mPrimitive ? TYPE_VALUES.get(mValueType) : TYPE_CLASSES.get(mValueType);
	}
}

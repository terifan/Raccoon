package org.terifan.raccoon.serialization.old;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


class FieldType implements Comparable<FieldType>, Externalizable
{
	private final static long serialVersionUID = 1L;

	private FieldCategory mCategory;
	private String mName;
	private Class mType;
	private boolean mNullable;
	private int mDepth;
	private FieldFormat mFormat;
	private FieldType[] mComponentType;


	public FieldType()
	{
	}


	@Override
	public void writeExternal(ObjectOutput aOut) throws IOException
	{
		aOut.writeObject(mName);
		aOut.writeObject(mType);
		aOut.writeObject(mCategory);
		aOut.writeObject(mNullable);
		aOut.writeObject(mFormat);
		aOut.writeObject(mDepth);
		aOut.writeObject(mComponentType);
	}


	@Override
	public void readExternal(ObjectInput aIn) throws IOException, ClassNotFoundException
	{
		mName = (String)aIn.readObject();
		mType = (Class)aIn.readObject();
		mCategory = (FieldCategory)aIn.readObject();
		mNullable = (Boolean)aIn.readObject();
		mFormat = (FieldFormat)aIn.readObject();
		mDepth = (Integer)aIn.readObject();
		mComponentType = (FieldType[])aIn.readObject();
	}


	@Override
	public int compareTo(FieldType aOther)
	{
		return mName.compareTo(aOther.mName);
	}


	@Override
	public String toString()
	{
		StringBuilder s = new StringBuilder();
		s.append(mName);
		for (int i = 0; i < mDepth; i++)
		{
			s.append("[]");
		}
		if (mComponentType != null)
		{
			s.append("<");
			s.append(mComponentType[0].getType().getSimpleName());
			if (mComponentType[0].mFormat == FieldFormat.ARRAY)
			{
				for (int i = 0; i < mComponentType[0].mDepth; i++)
				{
					s.append("[]");
				}
			}
			if (mComponentType[1] != null)
			{
				s.append("," + mComponentType[1].getType().getSimpleName());
				if (mComponentType[1].mFormat == FieldFormat.ARRAY)
				{
					for (int i = 0; i < mComponentType[1].mDepth; i++)
					{
						s.append("[]");
					}
				}
			}
			s.append(">");
		}
		s.append(" " + mName);
		if (mNullable)
		{
			s.append(" (nullable)");
		}
		return s.toString();
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


	public Class getType()
	{
		return mType;
	}


	public void setType(Class aType)
	{
		mType = aType;
	}


	public boolean isNullable()
	{
		return mNullable;
	}


	public void setNullable(boolean aNullable)
	{
		mNullable = aNullable;
	}


	public int getDepth()
	{
		return mDepth;
	}


	public void setDepth(int aDepth)
	{
		mDepth = aDepth;
	}


	public FieldFormat getFormat()
	{
		return mFormat;
	}


	public void setFormat(FieldFormat aFormat)
	{
		mFormat = aFormat;
	}


	public FieldType[] getComponentType()
	{
		return mComponentType;
	}


	public void setComponentType(FieldType[] aComponentType)
	{
		mComponentType = aComponentType;
	}
}

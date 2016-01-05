package org.terifan.raccoon;

import java.util.Arrays;
import java.util.HashMap;
import org.terifan.raccoon.serialization.Marshaller;
import org.terifan.raccoon.serialization.FieldCategory;
import org.terifan.raccoon.serialization.FieldType;
import org.terifan.raccoon.serialization.TypeDeclarations;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;


public final class TableMetadata
{
	@Discriminator private String mTypeGroup;
	@Key private String mTypeName;
	@Key private byte[] mDiscriminatorKey;
	private String mName;
	private byte[] mPointer;
	private TypeDeclarations mTypeDeclarations;

	private transient Class mClass;
	private transient Marshaller mMarshaller;


	TableMetadata()
	{
	}


	TableMetadata create(Class aClass, Object aDiscriminator)
	{
		mClass = aClass;
		mName = mClass.getSimpleName();
		mTypeName = mClass.getName();
		mTypeGroup = mTypeName;

		mMarshaller = new Marshaller(mClass);
		mTypeDeclarations = mMarshaller.getTypeDeclarations();

		mDiscriminatorKey = createDiscriminatorKey(aDiscriminator);

		return this;
	}


	TableMetadata initialize()
	{
		mMarshaller = new Marshaller(mTypeDeclarations);

		try
		{
			mClass = Class.forName(mTypeName);
		}
		catch (Exception e)
		{
			Log.out.println("Error loading entity class: " + e.toString());
		}

		return this;
	}


	byte[] createDiscriminatorKey(Object aDiscriminator)
	{
		if (aDiscriminator != null)
		{
			return mMarshaller.marshal(new ByteArrayBuffer(16), aDiscriminator, FieldCategory.DISCRIMINATORS).trim().array();
		}

		return new byte[0];
	}


	public String getName()
	{
		return mName;
	}


	public Class getType()
	{
		return mClass;
	}


	public String getTypeName()
	{
		return mTypeName;
	}


	public FieldType[] getFields()
	{
		return mTypeDeclarations.getTypes().clone();
	}


	public Marshaller getMarshaller()
	{
		return mMarshaller;
	}


	byte[] getDiscriminatorKey()
	{
		return mDiscriminatorKey;
	}


	byte[] getPointer()
	{
		return mPointer;
	}


	void setPointer(byte[] aPointer)
	{
		mPointer = aPointer;
	}


	@Override
	public boolean equals(Object aOther)
	{
		if (aOther instanceof TableMetadata)
		{
			TableMetadata other = (TableMetadata)aOther;

			return mTypeName.equals(other.mTypeName) && Arrays.equals(mDiscriminatorKey, other.mDiscriminatorKey);
		}

		return false;
	}


	@Override
	public int hashCode()
	{
		return mTypeName.hashCode() ^ Arrays.hashCode(mDiscriminatorKey);
	}


	@Override
	public String toString()
	{
		return mTypeName + "[" + getDiscriminatorDescription() + "]";
	}
	
	
	public String getDiscriminatorDescription()
	{
		if (mDiscriminatorKey.length == 0)
		{
			return null;
		}

		StringBuilder result = new StringBuilder();

		try
		{
			Marshaller marshaller = new Marshaller(mTypeDeclarations);
			HashMap<String, Object> map = marshaller.unmarshalDISCRIMINATORS(mDiscriminatorKey);

			for (FieldType type : mTypeDeclarations.getTypes())
			{
				if (type.getCategory() == FieldCategory.DISCRIMINATORS)
				{
					if (result.length() == 0)
					{
						result.append(", ");
					}
					result.append(type.getName()).append("=").append(map.get(type.getName()));
				}
			}
		}
		catch (Exception e)
		{
			Log.e("Error: %s", e.getMessage());
		}

		return result.toString();
	}


	public boolean hasDiscriminatorFields()
	{
		for (FieldType fieldType : mTypeDeclarations.getTypes())
		{
			if (fieldType.getCategory() == FieldCategory.DISCRIMINATORS)
			{
				return true;
			}
		}

		return false;
	}
}

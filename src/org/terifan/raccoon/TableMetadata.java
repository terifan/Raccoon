package org.terifan.raccoon;

import java.util.Arrays;
import org.terifan.raccoon.serialization.FieldCategory;
import org.terifan.raccoon.serialization.FieldCategoryFilter;
import org.terifan.raccoon.serialization.FieldType;
import org.terifan.raccoon.serialization.Marshaller;
import org.terifan.raccoon.serialization.EntityDescriptor;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;
import org.terifan.raccoon.util.ResultSet;


public final class TableMetadata
{
	@Key private String mTypeName;
	@Key private byte[] mDiscriminatorKey;
	private String mName;
	private byte[] mPointer;
	private EntityDescriptor mEntityDescriptor;

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

		mEntityDescriptor = new EntityDescriptor(mClass);
		mMarshaller = new Marshaller(mEntityDescriptor);

		mDiscriminatorKey = createDiscriminatorKey(aDiscriminator);

		return this;
	}


	TableMetadata initialize()
	{
		mMarshaller = new Marshaller(mEntityDescriptor);

		try
		{
			mClass = Class.forName(mTypeName);
		}
		catch (Exception e)
		{
			Log.out.println("Error loading entity class: " + e.toString());
		}

		if (mEntityDescriptor != null)
		{
			mEntityDescriptor.mapFields(mClass);
		}
		else 
			Log.out.println("warning: mEntityDescriptor is null");

		return this;
	}


	byte[] createDiscriminatorKey(Object aDiscriminator)
	{
		if (aDiscriminator != null)
		{
			return mMarshaller.marshal(new ByteArrayBuffer(16), aDiscriminator, FieldCategoryFilter.DISCRIMINATORS).trim().array();
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


	public EntityDescriptor getEntityDescriptor()
	{
		return mEntityDescriptor;
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
			Marshaller marshaller = new Marshaller(mEntityDescriptor);
			ResultSet resultSet = marshaller.unmarshal(mDiscriminatorKey, FieldCategoryFilter.DISCRIMINATORS);

			for (FieldType fieldType : mEntityDescriptor.getTypes())
			{
				if (fieldType.getCategory() == FieldCategory.DISCRIMINATOR)
				{
					if (result.length() == 0)
					{
						result.append(", ");
					}
					result.append(fieldType.getName()).append("=").append(resultSet.get(fieldType.getIndex()));
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
		for (FieldType fieldType : mEntityDescriptor.getTypes())
		{
			if (fieldType.getCategory() == FieldCategory.DISCRIMINATOR)
			{
				return true;
			}
		}

		return false;
	}
}

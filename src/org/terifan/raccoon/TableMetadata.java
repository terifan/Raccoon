package org.terifan.raccoon;

import java.util.Arrays;
import org.terifan.raccoon.serialization.FieldCategory;
import org.terifan.raccoon.serialization.FieldDescriptor;
import org.terifan.raccoon.serialization.Marshaller;
import org.terifan.raccoon.serialization.EntityDescriptor;
import org.terifan.raccoon.serialization.MarshallerRegistry;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;
import org.terifan.raccoon.util.ResultSet;


public final class TableMetadata
{
	@Key private String mTypeName;
	@Key private byte[] mDiscriminatorKey;
	private byte[] mPointer;
	private EntityDescriptor mEntityDescriptor;

	private transient Class mType;
	private transient Marshaller mMarshaller;


	TableMetadata()
	{
	}


	TableMetadata(Class aClass, Object aDiscriminator)
	{
		mType = aClass;
		mTypeName = mType.getName();
		mEntityDescriptor = EntityDescriptor.getInstance(mType);
		mMarshaller = MarshallerRegistry.getInstance(mEntityDescriptor);

		mDiscriminatorKey = createDiscriminatorKey(aDiscriminator);
	}


	synchronized TableMetadata initialize()
	{
		mMarshaller = MarshallerRegistry.getInstance(mEntityDescriptor);

		try
		{
			mType = Class.forName(mTypeName);
		}
		catch (Exception e)
		{
			Log.e("Error loading entity class: %s", e.toString());
		}

		if (mType != null)
		{
			mEntityDescriptor.setType(mType);
		}

		return this;
	}


	byte[] createDiscriminatorKey(Object aDiscriminator)
	{
		if (aDiscriminator != null)
		{
			return mMarshaller.marshalDiscriminators(new ByteArrayBuffer(16), aDiscriminator).trim().array();
		}

		return new byte[0];
	}


	public Class getType()
	{
		return mType;
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
		return mMarshaller.xxxxxxxxxxxxx(mType);
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
		String s = getDiscriminatorDescription();

		if (s == null)
		{
			return mTypeName;
		}

		return mTypeName + "[" + s + "]";
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
			Marshaller marshaller = MarshallerRegistry.getInstance(mEntityDescriptor);
			ResultSet resultSet = marshaller.unmarshalDiscriminators(new ByteArrayBuffer(mDiscriminatorKey));

			for (FieldDescriptor fieldType : mEntityDescriptor.getTypes())
			{
				if (fieldType.getCategory() == FieldCategory.DISCRIMINATOR)
				{
					if (result.length() > 0)
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
		for (FieldDescriptor fieldType : mEntityDescriptor.getTypes())
		{
			if (fieldType.getCategory() == FieldCategory.DISCRIMINATOR)
			{
				return true;
			}
		}

		return false;
	}
}

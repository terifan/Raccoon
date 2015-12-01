package org.terifan.raccoon;

import java.util.Arrays;
import org.terifan.raccoon.serialization.Marshaller;
import org.terifan.raccoon.serialization.FieldCategory;
import org.terifan.raccoon.serialization.FieldType;
import org.terifan.raccoon.serialization.TypeDeclarations;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;


public final class TableMetadata
{
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
		mMarshaller = new Marshaller(mClass);
		mDiscriminatorKey = createDiscriminatorKey(aDiscriminator);
		mTypeDeclarations = mMarshaller.getTypeDeclarations();
		
		return this;
	}


	// TODO: compare mTypeDeclarations and mMarshaller.mTypeDeclarations
	TableMetadata open()
	{
		try
		{
			mClass = Class.forName(mTypeName);
			mMarshaller = new Marshaller(mClass);
		}
		catch (Exception e)
		{
			e.printStackTrace(Log.out);
		}

		if (mMarshaller == null)
		{
			mMarshaller = new Marshaller(mTypeDeclarations);
		}
		
		return this;
	}


	byte[] createDiscriminatorKey(Object aDiscriminator)
	{
		if (aDiscriminator != null)
		{
			return mMarshaller.marshal(new ByteArrayBuffer(16), aDiscriminator, FieldCategory.DISCRIMINATOR).trim().array();
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


	public Iterable<FieldType> getFields()
	{
		return mTypeDeclarations;
	}


	Marshaller getMarshaller()
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
		return mTypeName;
	}
	
	
	public String getDiscriminatorDescription()
	{
		String d = "";

		if (mDiscriminatorKey.length > 0)
		{
			try
			{
				Object out = Class.forName(mTypeName).newInstance();
				
				mMarshaller = new Marshaller(mTypeDeclarations);
				
				mMarshaller.unmarshal(mDiscriminatorKey, out, FieldCategory.DISCRIMINATOR);

				for (FieldType type : mTypeDeclarations.getTypes())
				{
					if (type.getCategory() == FieldCategory.DISCRIMINATOR)
					{
						if (!d.isEmpty())
						{
							d += ", ";
						}
						d += type.getName() + "=" + out.getClass().getField(type.getName()).get(out);
					}
				}
			}
			catch (Exception e)
			{
				e.printStackTrace(Log.out);
			}
		}

		return d;
	}


	public boolean hasDiscriminatorFields()
	{
		for (FieldType fieldType : getFields())
		{
			if (fieldType.getCategory() == FieldCategory.DISCRIMINATOR)
			{
				return true;
			}
		}

		return false;
	}
}

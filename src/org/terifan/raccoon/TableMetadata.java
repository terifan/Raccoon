package org.terifan.raccoon;

import java.util.ArrayList;
import java.util.Arrays;
import org.terifan.raccoon.serialization.FieldDescriptor;
import org.terifan.raccoon.serialization.Marshaller;
import org.terifan.raccoon.serialization.EntityDescriptor;
import org.terifan.raccoon.serialization.FieldTypeCategorizer;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;
import org.terifan.raccoon.util.ResultSet;


public final class TableMetadata
{
	protected final static int FIELD_CATEGORY_KEY = 1;
	protected final static int FIELD_CATEGORY_DISCRIMINATOR = 2;
	protected final static int FIELD_CATEGORY_VALUE = 4;

	@Key private String mTypeName;
	@Key private byte[] mDiscriminatorKey;
	private byte[] mPointer;
	private EntityDescriptor mEntityDescriptor;

	private transient Class mType;
	private transient Marshaller mMarshaller;


	public TableMetadata()
	{
	}


	TableMetadata(Class aClass, DiscriminatorType aDiscriminator)
	{
		mType = aClass;
		mTypeName = mType.getName();
		mEntityDescriptor = new EntityDescriptor(mType, mCategorizer);
		mMarshaller = new Marshaller(mEntityDescriptor);

		mDiscriminatorKey = createDiscriminatorKey(aDiscriminator);

		if (mEntityDescriptor.getFields(FIELD_CATEGORY_KEY).isEmpty())
		{
			throw new IllegalArgumentException("Entity has no keys: " + aClass);
		}
	}


	synchronized TableMetadata initialize()
	{
		mMarshaller = new Marshaller(mEntityDescriptor);

		try
		{
			mType = Class.forName(mTypeName);
		}
		catch (Exception | Error e)
		{
			Log.e("Error loading entity class: %s", e.toString());
		}

		if (mType != null)
		{
			mEntityDescriptor.bind(mType);
		}

		return this;
	}

	
	public ArrayList<FieldDescriptor> getFields()
	{
		return mEntityDescriptor.getFields(FIELD_CATEGORY_KEY + FIELD_CATEGORY_DISCRIMINATOR + FIELD_CATEGORY_VALUE);
	}

	
	public ArrayList<FieldDescriptor> getKeyFields()
	{
		return mEntityDescriptor.getFields(FIELD_CATEGORY_KEY);
	}
	
	
	public ArrayList<FieldDescriptor> getDiscriminatorFields()
	{
		return mEntityDescriptor.getFields(FIELD_CATEGORY_DISCRIMINATOR);
	}
	
	
	public ArrayList<FieldDescriptor> getValueFields()
	{
		return mEntityDescriptor.getFields(FIELD_CATEGORY_VALUE);
	}


	byte[] createDiscriminatorKey(DiscriminatorType aDiscriminator)
	{
		if (aDiscriminator != null && aDiscriminator.getInstance() != null)
		{
			return mMarshaller.marshal(new ByteArrayBuffer(16), aDiscriminator.getInstance(), TableMetadata.FIELD_CATEGORY_DISCRIMINATOR).trim().array();
		}

		return new byte[0];
	}


	Marshaller getMarshaller()
	{
		return mMarshaller;
	}


	public Class getType()
	{
		return mType;
	}


	public String getTypeName()
	{
		return mTypeName;
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

		Marshaller marshaller = new Marshaller(mEntityDescriptor);
		ResultSet resultSet = marshaller.unmarshal(new ByteArrayBuffer(mDiscriminatorKey), new ResultSet(), TableMetadata.FIELD_CATEGORY_DISCRIMINATOR);
		StringBuilder result = new StringBuilder();

		for (FieldDescriptor fieldType : mEntityDescriptor.getFields(FIELD_CATEGORY_DISCRIMINATOR))
		{
			if (result.length() > 0)
			{
				result.append(", ");
			}

			result.append(fieldType.getName()).append("=").append(resultSet.get(fieldType.getIndex()));
		}

		return result.toString();
	}


	/**
	 * Return an entity as a Java class declaration.
	 */
	public String getJavaDeclaration()
	{
		StringBuilder sb = new StringBuilder();
		sb.append("package " + mEntityDescriptor.getName().substring(0, mEntityDescriptor.getName().lastIndexOf('.')) + ";\n\n");
		sb.append("class " + mEntityDescriptor.getName().substring(mEntityDescriptor.getName().lastIndexOf('.') + 1) + "\n{\n");

		for (FieldDescriptor fieldType : mEntityDescriptor.getFields(FIELD_CATEGORY_KEY))
		{
			sb.append("\t" + "@Key " + fieldType + ";\n");
		}
		for (FieldDescriptor fieldType : mEntityDescriptor.getFields(FIELD_CATEGORY_DISCRIMINATOR))
		{
			sb.append("\t" + "@Discriminator " + fieldType + ";\n");
		}
		for (FieldDescriptor fieldType : mEntityDescriptor.getFields(FIELD_CATEGORY_VALUE))
		{
			sb.append("\t" + "" + fieldType + ";\n");
		}

		sb.append("}");

		return sb.toString();
	}


	private transient FieldTypeCategorizer mCategorizer = aField ->
	{
		if (aField.getAnnotation(Key.class) != null)
		{
			return FIELD_CATEGORY_KEY;
		}
		if (aField.getAnnotation(Discriminator.class) != null)
		{
			return FIELD_CATEGORY_DISCRIMINATOR;
		}
		return FIELD_CATEGORY_VALUE;
	};


	protected EntityDescriptor getEntityDescriptor()
	{
		return mEntityDescriptor;
	}


	public static String getCategoryName(int aCategory)
	{
		if ((aCategory & FIELD_CATEGORY_KEY) != 0)
		{
			return "Key";
		}
		if ((aCategory & FIELD_CATEGORY_DISCRIMINATOR) != 0)
		{
			return "Discriminator";
		}
		return "Value";
	}
}

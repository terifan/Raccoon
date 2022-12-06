package org.terifan.raccoon;

import org.terifan.raccoon.annotations.Discriminator;
import org.terifan.raccoon.annotations.Id;
import java.util.ArrayList;
import java.util.Arrays;
import org.terifan.raccoon.annotations.Column;
import org.terifan.raccoon.annotations.Entity;
import org.terifan.raccoon.serialization.FieldDescriptor;
import org.terifan.raccoon.serialization.Marshaller;
import org.terifan.raccoon.serialization.EntityDescriptor;
import org.terifan.raccoon.serialization.FieldTypeCategorizer;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;


@Entity
public final class Table<T>
{
	public final static int FIELD_CATEGORY_ID = 1;
	public final static int FIELD_CATEGORY_DISCRIMINATOR = 2;
	public final static int FIELD_CATEGORY_VALUE = 4;

	@Id private String mEntityName;
	@Id private byte[] mDiscriminatorKey;
	@Column private EntityDescriptor mEntityDescriptor;
	@Column private byte[] mTableHeader;
	@Column private String mImplementation;

	private transient Class mType;
	private transient Marshaller mMarshaller;


	Table()
	{
	}


	Table(Database aDatabase, Class aClass, DiscriminatorType aDiscriminator)
	{
		mType = aClass;

		Entity entity = (Entity)mType.getAnnotation(Entity.class);
		if (entity != null)
		{
			mEntityName = entity.name();
			mImplementation = entity.implementation();
		}
		else
		{
			mEntityName = mType.getName();
			mImplementation = Constants.DEFAULT_TABLE_IMPLEMENTATION;
		}

		mEntityDescriptor = new EntityDescriptor(mEntityName, mType, mCategorizer);
		mMarshaller = new Marshaller(mEntityDescriptor);

		mDiscriminatorKey = createDiscriminatorKey(aDiscriminator);

		if (mEntityDescriptor.getFields(FIELD_CATEGORY_ID).isEmpty())
		{
			throw new IllegalArgumentException("Entity has no keys: " + aClass);
		}
	}


	synchronized Table initialize(Database aDatabase)
	{
		mMarshaller = new Marshaller(mEntityDescriptor);

		try
		{
			mType = Class.forName(mEntityName);
		}
		catch (Exception | Error e)
		{
			Log.w("Warning: entity class not accessible: %s", e.toString());
		}

		if (mType != null)
		{
			mEntityDescriptor.bind(mType);
		}

		return this;
	}


	public FieldDescriptor[] getFields()
	{
		return mEntityDescriptor.getFields();
	}


	public ArrayList<FieldDescriptor> getKeyFields()
	{
		return mEntityDescriptor.getFields(FIELD_CATEGORY_ID);
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
			return mMarshaller.marshal(ByteArrayBuffer.alloc(16), aDiscriminator.getInstance(), Table.FIELD_CATEGORY_DISCRIMINATOR).trim().array();
		}

		return new byte[0];
	}


	Marshaller getMarshaller()
	{
		return mMarshaller;
	}


	public Class getType()
	{
		if (mType == null)
		{
			try
			{
				mType = Class.forName(getTypeName());
			}
			catch (Exception e)
			{
				// ignore
			}
		}
		return mType;
	}


	public String getEntityName()
	{
		return mEntityName;
	}


	public String getTypeName()
	{
		return mEntityDescriptor.getTypeName();
	}


	byte[] getDiscriminatorKey()
	{
		return mDiscriminatorKey;
	}


	byte[] getTableHeader()
	{
		return mTableHeader;
	}


	void setTableHeader(byte[] aTableHeader)
	{
		mTableHeader = aTableHeader;
	}


	@Override
	public boolean equals(Object aOther)
	{
		if (aOther instanceof Table)
		{
			Table other = (Table)aOther;

			return mEntityName.equals(other.mEntityName) && Arrays.equals(mDiscriminatorKey, other.mDiscriminatorKey);
		}

		return false;
	}


	@Override
	public int hashCode()
	{
		return mEntityName.hashCode() ^ Arrays.hashCode(mDiscriminatorKey);
	}


	@Override
	public String toString()
	{
		String s = getDiscriminatorDescription();

		if (s == null)
		{
			return mEntityName;
		}

		return mEntityName + "[" + s + "]";
	}


	public String getDiscriminatorDescription()
	{
		if (mDiscriminatorKey.length == 0)
		{
			return null;
		}

		ResultSet resultSet = new ResultSet(mEntityDescriptor).unmarshal(ByteArrayBuffer.wrap(mDiscriminatorKey), FIELD_CATEGORY_DISCRIMINATOR);

		StringBuilder result = new StringBuilder();

		for (FieldDescriptor fieldType : mEntityDescriptor.getFields(FIELD_CATEGORY_DISCRIMINATOR))
		{
			if (result.length() > 0)
			{
				result.append(", ");
			}

			result.append(fieldType.getFieldName()).append("=").append(resultSet.get(fieldType));
		}

		return result.toString();
	}


	/**
	 * Return an entity as a Java class declaration.
	 */
	public String getJavaDeclaration()
	{
		StringBuilder sb = new StringBuilder();
		sb.append("package " + mEntityDescriptor.getPackageName() + ";\n\n");

		String className = mEntityDescriptor.getTypeName().substring(mEntityDescriptor.getTypeName().lastIndexOf('.') + 1);
		if (className.contains("$"))
		{
			className = className.substring(className.lastIndexOf("$") + 1);
		}

		if (mEntityDescriptor.getEntityName().length() > 0 && !mEntityDescriptor.getEntityName().equals(mEntityDescriptor.getTypeName()))
		{
			sb.append("@Entity(name = \"" + mEntityDescriptor.getEntityName() + "\")\n");
		}
		else
		{
			sb.append("@Entity\n");
		}

		sb.append("class " + className + "\n{\n");

		for (FieldDescriptor fieldType : mEntityDescriptor.getFields(FIELD_CATEGORY_ID))
		{
			String tmp = fieldType.getColumnName().isEmpty() ? "" : "(name = \"" + fieldType.getColumnName() + "\")";
			sb.append("\t" + "@" + Id.class.getSimpleName() + tmp + " " + fieldType.toTypeNameString() + ";\n");
		}
		for (FieldDescriptor fieldType : mEntityDescriptor.getFields(FIELD_CATEGORY_DISCRIMINATOR))
		{
			String tmp = fieldType.getColumnName().isEmpty() ? "" : "(name = \"" + fieldType.getColumnName() + "\")";
			sb.append("\t" + "@" + Discriminator.class.getSimpleName() + tmp + " " + fieldType.toTypeNameString() + ";\n");
		}
		for (FieldDescriptor fieldType : mEntityDescriptor.getFields(FIELD_CATEGORY_VALUE))
		{
			String tmp = fieldType.getColumnName().isEmpty() ? "" : "(name = \"" + fieldType.getColumnName() + "\")";
			sb.append("\t" + "@" + Column.class.getSimpleName() + tmp + " " + fieldType.toTypeNameString() + ";\n");
		}

		sb.append("}");

		return sb.toString();
	}


	private transient FieldTypeCategorizer mCategorizer = aField ->
	{
		if (aField.getAnnotation(Id.class) != null)
		{
			return FIELD_CATEGORY_ID;
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
		if ((aCategory & FIELD_CATEGORY_ID) != 0)
		{
			return "Key";
		}
		if ((aCategory & FIELD_CATEGORY_DISCRIMINATOR) != 0)
		{
			return "Discriminator";
		}
		return "Value";
	}


	public String getImplementation()
	{
		return mImplementation;
	}


	public interface ResultSetConsumer
	{
		void handle(ResultSet aResultSet);
	}
}

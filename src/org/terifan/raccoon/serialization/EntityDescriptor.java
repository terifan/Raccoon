package org.terifan.raccoon.serialization;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Objects;
import org.terifan.raccoon.util.Log;


/**
 * 
 */
public class EntityDescriptor implements Externalizable
{
	private static final long serialVersionUID = 1L;

	private String mName;
	private FieldDescriptor[] mKeyFields;
	private FieldDescriptor[] mDiscriminatorFields;
	private FieldDescriptor[] mValueFields;


	public EntityDescriptor()
	{
	}


	EntityDescriptor(Class aType, FieldDescriptor[] aKeyFields, FieldDescriptor[] aDiscriminatorFields, FieldDescriptor[] aValueFields)
	{
		Log.v("create type declarations for %s", aType);
		Log.inc();

		mName = aType.getName();
		mKeyFields = aKeyFields;
		mDiscriminatorFields = aDiscriminatorFields;
		mValueFields = aValueFields;

		Log.dec();
	}


	public void setType(Class aType)
	{
		for (Field field : ObjectReflection.getDeclaredFields(aType))
		{
			for (FieldDescriptor[] fieldDescriptors : new FieldDescriptor[][]{mKeyFields, mDiscriminatorFields, mValueFields})
			{
				for (FieldDescriptor fieldDescriptor : fieldDescriptors)
				{
					if (fieldDescriptor.getName().equals(field.getName()) && fieldDescriptor.getTypeName().equals(field.getType().getName()))
					{
						fieldDescriptor.setField(field);
					}
				}
			}
		}
	}


	public String getName()
	{
		return mName;
	}


	public FieldDescriptor[] getKeyFields()
	{
		return mKeyFields;
	}


	public FieldDescriptor[] getDiscriminatorFields()
	{
		return mDiscriminatorFields;
	}


	public FieldDescriptor[] getValueFields()
	{
		return mValueFields;
	}


	@Override
	public void readExternal(ObjectInput aIn) throws IOException, ClassNotFoundException
	{
		mName = aIn.readUTF();
		mKeyFields = (FieldDescriptor[])aIn.readObject();
		mDiscriminatorFields = (FieldDescriptor[])aIn.readObject();
		mValueFields = (FieldDescriptor[])aIn.readObject();
	}


	@Override
	public void writeExternal(ObjectOutput aOut) throws IOException
	{
		aOut.writeUTF(mName);
		aOut.writeObject(mKeyFields);
		aOut.writeObject(mDiscriminatorFields);
		aOut.writeObject(mValueFields);
	}


	@Override
	public boolean equals(Object aObj)
	{
		if (aObj instanceof EntityDescriptor)
		{
			EntityDescriptor other = (EntityDescriptor)aObj;

			return mName.equals(other.mName) && Arrays.equals(mKeyFields, other.mKeyFields) && Arrays.equals(mDiscriminatorFields, other.mDiscriminatorFields) && Arrays.equals(mValueFields, other.mValueFields);
		}

		return false;
	}


	@Override
	public int hashCode()
	{
		return Objects.hashCode(mName) ^ Arrays.deepHashCode(mKeyFields) ^ Arrays.deepHashCode(mDiscriminatorFields) ^ Arrays.deepHashCode(mValueFields);
	}


	/**
	 * Return an entity as a Java class declaration.
	 */
	public String getJavaDeclaration()
	{
		StringBuilder sb = new StringBuilder();
		sb.append("package " + mName.substring(0, mName.lastIndexOf('.')) + ";\n\n");
		sb.append("class " + mName.substring(mName.lastIndexOf('.') + 1) + "\n{\n");

		for (FieldDescriptor fieldType : mKeyFields)
		{
			sb.append("\t" + "@Key " + fieldType + ";\n");
		}
		for (FieldDescriptor fieldType : mDiscriminatorFields)
		{
			sb.append("\t" + "@Discriminator " + fieldType + ";\n");
		}
		for (FieldDescriptor fieldType : mValueFields)
		{
			sb.append("\t" + "" + fieldType + ";\n");
		}

		sb.append("}");

		return sb.toString();
	}


	@Override
	public String toString()
	{
		return "EntityDescriptor{" + "mName=" + mName + ", mKeyFields=" + Arrays.toString(mKeyFields) + ", mDiscriminatorFields=" + Arrays.toString(mDiscriminatorFields) + ", mValueFields=" + Arrays.toString(mValueFields) + '}';
	}
}

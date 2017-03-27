package org.terifan.raccoon.serialization;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;
import org.terifan.raccoon.util.Log;


/**
 * 
 */
public class EntityDescriptor implements Externalizable
{
	private String mName;
	private FieldDescriptor[] mFields;


	public EntityDescriptor()
	{
	}


	EntityDescriptor(Class aType, FieldDescriptor[] aFields)
	{
		Log.v("create type declarations for %s", aType);
		Log.inc();

		mName = aType.getName();
		mFields = aFields;

		Log.dec();
	}


	public void setType(Class aType)
	{
		for (Field field : ObjectReflection.getDeclaredFields(aType))
		{
			for (FieldDescriptor fieldDescriptor : mFields)
			{
				if (fieldDescriptor.getName().equals(field.getName()) && fieldDescriptor.getTypeName().equals(field.getType().getName()))
				{
					fieldDescriptor.setField(field);
				}
			}
		}
	}


	public String getName()
	{
		return mName;
	}


	public FieldDescriptor[] getFields()
	{
		return mFields;
	}


	/**
	 * Return fields of one or more categories
	 * 
	 * @param aCategory
	 *   bit field for category
	 */
	public ArrayList<FieldDescriptor> getFields(int aCategory)
	{
		ArrayList<FieldDescriptor> fd = new ArrayList<>();
		for (FieldDescriptor field : mFields)
		{
			if ((field.getCategory() & aCategory) != 0)
			{
				fd.add(field);
			}
		}
		return fd;
	}


	@Override
	public void readExternal(ObjectInput aIn) throws IOException, ClassNotFoundException
	{
		mName = aIn.readUTF();
		mFields = (FieldDescriptor[])aIn.readObject();
	}


	@Override
	public void writeExternal(ObjectOutput aOut) throws IOException
	{
		aOut.writeUTF(mName);
		aOut.writeObject(mFields);
	}


	@Override
	public boolean equals(Object aObj)
	{
		if (aObj instanceof EntityDescriptor)
		{
			EntityDescriptor other = (EntityDescriptor)aObj;

			return mName.equals(other.mName) && Arrays.equals(mFields, other.mFields);
		}

		return false;
	}


	@Override
	public int hashCode()
	{
		return Objects.hashCode(mName) ^ Arrays.deepHashCode(mFields);
	}


	@Override
	public String toString()
	{
		return "EntityDescriptor{" + "mName=" + mName + ", mFields=" + Arrays.toString(mFields) + '}';
	}
}

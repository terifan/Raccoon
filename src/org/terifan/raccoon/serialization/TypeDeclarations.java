package org.terifan.raccoon.serialization;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Field;
import java.util.Date;
import org.terifan.raccoon.Discriminator;
import org.terifan.raccoon.Key;
import org.terifan.raccoon.util.Log;


public class TypeDeclarations implements Externalizable
{
	private final static long serialVersionUID = 1L;

	private String mName;
	private FieldType[] mFields;


	public TypeDeclarations()
	{
	}


	public TypeDeclarations(Class aType, Field[] aFields)
	{
		Log.v("create type declarations for %s", aType);
		Log.inc();

		mName = aType.getName();
		mFields = new FieldType[aFields.length];

		int fieldIndex = 0;
		for (Field field : aFields)
		{
			FieldType fieldType = new FieldType();

			fieldType.setName(field.getName());

			categorizeContentType(field, fieldType);
			classifyContentType(field, fieldType);

			mFields[fieldIndex++] = fieldType;

			Log.v("type found: %s", fieldType);
		}

		Log.dec();
	}


	public FieldType[] getTypes()
	{
		return mFields;
	}


	public String getName()
	{
		return mName;
	}


	@Override
	public void readExternal(ObjectInput aIn) throws IOException, ClassNotFoundException
	{
		mName = aIn.readUTF();
		mFields = new FieldType[aIn.readShort()];
		for (int i = 0; i < mFields.length; i++)
		{
			mFields[i] = new FieldType();
			mFields[i].readExternal(aIn);
		}
	}


	@Override
	public void writeExternal(ObjectOutput aOut) throws IOException
	{
		aOut.writeUTF(mName);
		aOut.writeShort(mFields.length);
		for (FieldType fieldType : mFields)
		{
			fieldType.writeExternal(aOut);
		}
	}


	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		for (FieldType fieldType : mFields)
		{
			if (sb.length() > 0)
			{
				sb.append("\n");
			}
			sb.append(fieldType);
		}
		return sb.toString();
	}


	private void categorizeContentType(Field aField, FieldType aFieldType)
	{
		if (aField.getAnnotation(Discriminator.class) != null)
		{
			aFieldType.setCategory(FieldCategory.DISCRIMINATOR);
		}
		else if (aField.getAnnotation(Key.class) != null)
		{
			aFieldType.setCategory(FieldCategory.KEY);
		}
		else
		{
			aFieldType.setCategory(FieldCategory.VALUE);
		}
	}


	private void classifyContentType(Field aField, FieldType aFieldType)
	{
		Class<?> type = aField.getType();

		if (type.isPrimitive())
		{
			ContentType primitiveType = ContentType.types.get(type);
			if (primitiveType != null)
			{
				aFieldType.setContentType(primitiveType);
				return;
			}
		}

		aFieldType.setNullable(true);

		if (type.isArray())
		{
			ContentType componentType = ContentType.types.get(type.getComponentType());
			if (componentType != null)
			{
				aFieldType.setArray(true);
				aFieldType.setContentType(componentType);
				return;
			}

			if (Date.class.isAssignableFrom(type.getComponentType()))
			{
				aFieldType.setArray(true);
				aFieldType.setContentType(ContentType.DATE);
				return;
			}
		}

		ContentType classType = ContentType.classTypes.get(type);
		if (classType != null)
		{
			aFieldType.setContentType(classType);
			return;
		}

		if (Date.class.isAssignableFrom(type))
		{
			aFieldType.setContentType(ContentType.DATE);
			return;
		}

		aFieldType.setContentType(ContentType.OBJECT);
	}
}
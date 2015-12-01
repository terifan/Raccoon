package org.terifan.raccoon.serialization;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import org.terifan.raccoon.DatabaseException;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;


public class Marshaller
{
	private HashMap<String, Field> mFields;
	private TypeDeclarations mTypeDeclarations;


	public Marshaller(TypeDeclarations aTypeDeclarations)
	{
		mTypeDeclarations = aTypeDeclarations;
	}


	public Marshaller(Class aType)
	{
		if (aType != null)
		{
			loadFields(aType);
		}

		mTypeDeclarations = new TypeDeclarations(aType, mFields);
	}


	@Deprecated
	public byte[] marshal(Object aObject, FieldCategory aFieldCategory)
	{
		return marshal(new ByteArrayBuffer(16), aObject, aFieldCategory).trim().array();
	}


	public ByteArrayBuffer marshal(ByteArrayBuffer aBuffer, Object aObject, FieldCategory aFieldCategory)
	{
		if (aObject != null && mFields == null)
		{
			loadFields(aObject.getClass());
		}

		try
		{
			Log.v("marshal entity fields %s", aFieldCategory);
			Log.inc();

			for (FieldType fieldType : mTypeDeclarations)
			{
				if (fieldType.category == aFieldCategory || aFieldCategory == FieldCategory.DISCRIMINATOR_VALUE && (fieldType.category == FieldCategory.VALUE || fieldType.category == FieldCategory.DISCRIMINATOR))
				{
					Field field = findField(fieldType);
					Object value = field.get(aObject);
					FieldWriter.writeField(fieldType, aBuffer, value);
				}
			}

			Log.dec();

			return aBuffer;
		}
		catch (IllegalAccessException e)
		{
			throw new DatabaseException(e);
		}
	}


	@Deprecated
	public void unmarshal(byte[] aBuffer, Object aOutputObject, FieldCategory aFieldCategory) throws IOException
	{
		unmarshal(new ByteArrayBuffer(aBuffer), aOutputObject, aFieldCategory);
	}


	public void unmarshal(ByteArrayBuffer aBuffer, Object aOutputObject, FieldCategory aFieldCategory) throws IOException
	{
		if (aOutputObject != null && mFields == null)
		{
			loadFields(aOutputObject.getClass());
		}

		try
		{
			Log.v("unmarshal entity");
			Log.inc();

			for (FieldType fieldType : mTypeDeclarations)
			{
				if (fieldType.category == aFieldCategory || aFieldCategory == FieldCategory.DISCRIMINATOR_VALUE && (fieldType.category == FieldCategory.VALUE || fieldType.category == FieldCategory.DISCRIMINATOR))
				{
					Field field = findField(fieldType);

					Object value = FieldReader.readField(fieldType, aBuffer, field);

					if (field != null)
					{
						field.set(aOutputObject, value);
					}
				}
			}

			Log.dec();
		}
		catch (IllegalAccessException e)
		{
			throw new DatabaseException("Failed to reconstruct entity: " + (aOutputObject == null ? null : aOutputObject.getClass()), e);
		}
	}


	public TypeDeclarations getTypeDeclarations()
	{
		return mTypeDeclarations;
	}


	private Field findField(FieldType aFieldType)
	{
		return mFields.get(aFieldType.name);
	}


	private void loadFields(Class aType) throws SecurityException
	{
		mFields = new HashMap<>();

		for (Field field : aType.getDeclaredFields())
		{
			if ((field.getModifiers() & (Modifier.TRANSIENT | Modifier.STATIC | Modifier.FINAL)) != 0)
			{
				continue;
			}

			field.setAccessible(true);

			mFields.put(field.getName(), field);
		}
	}
}
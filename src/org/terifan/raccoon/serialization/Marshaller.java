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


	public Marshaller(TypeDeclarations aTypeDeclarations) throws IOException
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


	public byte[] marshal(Object aObject, FieldCategory aFieldCategory)
	{
		if (aObject != null && mFields == null)
		{
			loadFields(aObject.getClass());
		}

		try
		{
			Log.v("marshal entity fields " + aFieldCategory);
			Log.inc();

			ByteArrayBuffer out = new ByteArrayBuffer(64);

			for (FieldType fieldType : mTypeDeclarations)
			{
				if (fieldType.category == aFieldCategory)
				{
					Field field = findField(fieldType);
					Object value = field.get(aObject);
					FieldWriter.writeField(fieldType, out, value);
				}
			}

			Log.dec();

			return out.trim().array();
		}
		catch (IOException | IllegalAccessException e)
		{
			throw new DatabaseException(e);
		}
	}


	public void unmarshal(byte[] aBuffer, Object aObject, FieldCategory aFieldCategory)
	{
		if (aBuffer.length == 0)
		{
			return;
		}

		if (aObject != null && mFields == null)
		{
			loadFields(aObject.getClass());
		}

		try
		{
			Log.v("unmarshal entity");
			Log.inc();

			ByteArrayBuffer in = new ByteArrayBuffer(aBuffer);

			for (FieldType fieldType : mTypeDeclarations)
			{
				if (fieldType.category == aFieldCategory)
				{
					Field field = findField(fieldType);
					Object value = FieldReader.readField(fieldType, in);
					if (field != null)
					{
						field.set(aObject, value);
					}
				}
			}

			Log.dec();
		}
		catch (IOException | IllegalAccessException e)
		{
			throw new DatabaseException("Failed to reconstruct entity: " + (aObject == null ? null : aObject.getClass()), e);
		}
	}


	public TypeDeclarations getTypeDeclarations() throws IOException
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
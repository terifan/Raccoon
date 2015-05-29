package org.terifan.raccoon.serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import org.terifan.raccoon.DatabaseException;
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

			ByteArrayOutputStream baos = new ByteArrayOutputStream();

			try (DataOutputStream out = new DataOutputStream(baos))
			{
				for (int index = 0; index < mTypeDeclarations.size(); index++)
				{
					FieldType typeInfo = mTypeDeclarations.get(index);

					if (typeInfo.category == aFieldCategory)
					{
						Field field = findField(typeInfo);
						Object value = field.get(aObject);
						FieldWriter.writeField(typeInfo, out, value);
					}
				}

				out.writeInt(-1);
			}

			Log.dec();

			return baos.toByteArray();
		}
		catch (IOException | IllegalAccessException e)
		{
			throw new DatabaseException(e);
		}
	}


	public void unmarshal(byte[] aBuffer, Object aObject)
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

			try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(aBuffer)))
			{
				for (int i = 0; i < mTypeDeclarations.size(); i++)
				{
					FieldType typeInfo = mTypeDeclarations.get(i);
					FieldReader.readField(typeInfo, in, findField(typeInfo), aObject);
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


	private Field findField(FieldType aTypeInfo)
	{
		return mFields.get(aTypeInfo.name);
	}


	private void loadFields(Class aType) throws SecurityException
	{
		mFields = new HashMap<>();

		for (Field field : aType.getDeclaredFields())
		{
			field.setAccessible(true);

			if ((field.getModifiers() & (Modifier.TRANSIENT | Modifier.STATIC | Modifier.FINAL)) != 0)
			{
				continue;
			}

			mFields.put(field.getName(), field);
		}
	}
}
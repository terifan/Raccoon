package org.terifan.raccoon.serialization;

import java.lang.reflect.Field;
import java.util.Collection;
import org.terifan.raccoon.DatabaseException;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;
import org.terifan.raccoon.util.ResultSet;


public class Marshaller
{
	private EntityDescriptor mEntityDescriptor;


	public Marshaller(EntityDescriptor aTypeDeclarations)
	{
		mEntityDescriptor = aTypeDeclarations;
	}


	@Deprecated
	public byte[] marshal(Object aObject, Collection<FieldCategory> aFieldCategories)
	{
		return marshal(new ByteArrayBuffer(16), aObject, aFieldCategories).trim().array();
	}


	public ByteArrayBuffer marshal(ByteArrayBuffer aBuffer, Object aObject, Collection<FieldCategory> aFieldCategories)
	{
		try
		{
			Log.v("marshal entity fields %s", aFieldCategories);
			Log.inc();

			for (FieldType fieldType : mEntityDescriptor.getTypes())
			{
				if (aFieldCategories.contains(fieldType.getCategory()))
				{
					Object value = fieldType.getField().get(aObject);

					if (value != null)
					{
						aBuffer.write(0);
						FieldWriter.writeField(fieldType, value, aBuffer);
					}
					else
					{
						aBuffer.write(1);
					}
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
	public void unmarshal(byte[] aBuffer, Object aOutputObject, Collection<FieldCategory> aFieldCategories)
	{
		unmarshal(new ByteArrayBuffer(aBuffer), aOutputObject, aFieldCategories);
	}


	public void unmarshal(ByteArrayBuffer aBuffer, Object aObject, Collection<FieldCategory> aFieldCategories)
	{
		try
		{
			Log.v("unmarshal entity fields");
			Log.inc();

			FieldType[] types = mEntityDescriptor.getTypes();

			for (FieldType fieldType : types)
			{
				if (aFieldCategories.contains(fieldType.getCategory()))
				{
					if (aBuffer.read() == 0)
					{
						Object value = FieldReader.readField(fieldType, aBuffer);

						if (aObject != null)
						{
							if (fieldType.getField() != null)
							{
								fieldType.getField().set(aObject, value);
							}
							else
							{
								// todo
							}
						}
					}
				}
			}

			Log.dec();
		}
		catch (IllegalAccessException e)
		{
			throw new DatabaseException(e);
		}
	}


	@Deprecated
	public ResultSet unmarshal(byte[] aBuffer, Collection<FieldCategory> aFieldCategories)
	{
		return unmarshal(new ByteArrayBuffer(aBuffer), aFieldCategories);
	}


	public ResultSet unmarshal(ByteArrayBuffer aBuffer, Collection<FieldCategory> aFieldCategories)
	{
		Log.v("unmarshal entity fields");
		Log.inc();

		ResultSet resultSet = new ResultSet();
		FieldType[] types = mEntityDescriptor.getTypes();

		for (FieldType fieldType : types)
		{
			if (aFieldCategories.contains(fieldType.getCategory()))
			{
				if (aBuffer.read() == 0)
				{
					Object value = FieldReader.readField(fieldType, aBuffer);

					resultSet.add(fieldType, value);
				}
			}
		}

		Log.dec();

		return resultSet;
	}
}
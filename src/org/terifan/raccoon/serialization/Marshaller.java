package org.terifan.raccoon.serialization;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import org.terifan.raccoon.DatabaseException;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;
import org.terifan.raccoon.util.ResultSet;


public class Marshaller
{
	private EntityDescriptor mEntityDescriptor;


	Marshaller(EntityDescriptor aTypeDeclarations)
	{
		mEntityDescriptor = aTypeDeclarations;
	}


	public ByteArrayBuffer marshalKeys(ByteArrayBuffer aBuffer, Object aObject)
	{
		return marshalImpl(aBuffer, aObject, mEntityDescriptor.getKeyFields());
	}


	public ByteArrayBuffer marshalDiscriminators(ByteArrayBuffer aBuffer, Object aObject)
	{
		return marshalImpl(aBuffer, aObject, mEntityDescriptor.getDiscriminatorFields());
	}


	public ByteArrayBuffer marshalValues(ByteArrayBuffer aBuffer, Object aObject)
	{
		return marshalImpl(aBuffer, aObject, mEntityDescriptor.getValueFields());
	}


	public void unmarshalKeys(ByteArrayBuffer aBuffer, Object aObject)
	{
		unmarshalImpl(aBuffer, aObject, mEntityDescriptor.getKeyFields());
	}


	public void unmarshalDiscriminators(ByteArrayBuffer aBuffer, Object aObject)
	{
		unmarshalImpl(aBuffer, aObject, mEntityDescriptor.getDiscriminatorFields());
	}


	public void unmarshalValues(ByteArrayBuffer aBuffer, Object aObject)
	{
		unmarshalImpl(aBuffer, aObject, mEntityDescriptor.getValueFields());
	}


	public ResultSet unmarshalKeys(ByteArrayBuffer aBuffer)
	{
		return unmarshalImpl(aBuffer, mEntityDescriptor.getKeyFields());
	}


	public ResultSet unmarshalDiscriminators(ByteArrayBuffer aBuffer)
	{
		return unmarshalImpl(aBuffer, mEntityDescriptor.getDiscriminatorFields());
	}


	public ResultSet unmarshalValues(ByteArrayBuffer aBuffer)
	{
		return unmarshalImpl(aBuffer, mEntityDescriptor.getValueFields());
	}


	private ByteArrayBuffer marshalImpl(ByteArrayBuffer aBuffer, Object aObject, FieldDescriptor[] aTypes)
	{
		try
		{
			Log.v("marshal entity fields %s", Arrays.toString(aTypes));
			Log.inc();
			
			ArrayList<Object> values = new ArrayList<>();

			// write null-bitmap
			for (FieldDescriptor fieldType : aTypes)
			{
				Object value = fieldType.getField().get(aObject);

				aBuffer.writeBit(value == null);

				values.add(value);
			}

			aBuffer.align();

			for (int i = 0; i < aTypes.length; i++)
			{
				Object value = values.get(i);

				if (value != null)
				{
					FieldWriter.writeField(aTypes[i], value, aBuffer);
				}
			}

			Log.dec();

			return aBuffer;
		}
		catch (IllegalAccessException | IOException e)
		{
			throw new DatabaseException(e);
		}
	}


	private void unmarshalImpl(ByteArrayBuffer aBuffer, Object aObject, FieldDescriptor[] aTypes)
	{
		try
		{
			Log.v("unmarshal entity fields");
			Log.inc();

			ArrayList<FieldDescriptor> readFields = new ArrayList<>();

			for (int i = 0; i < aTypes.length; i++)
			{
				if (aBuffer.readBit() == 0)
				{
					readFields.add(aTypes[i]);
				}
			}

			aBuffer.align();

			for (FieldDescriptor fieldType : readFields)
			{
				Object value = FieldReader.readField(fieldType, aBuffer);

				if (aObject != null)
				{
					Field field = fieldType.getField();

					if (field != null)
					{
						field.set(aObject, value);
					}
				}
			}

			Log.dec();
		}
		catch (IllegalAccessException | IOException | ClassNotFoundException e)
		{
			throw new DatabaseException(e);
		}
	}


	private ResultSet unmarshalImpl(ByteArrayBuffer aBuffer, FieldDescriptor[] types)
	{
		try
		{
			Log.v("unmarshal entity fields");
			Log.inc();

			ResultSet resultSet = new ResultSet();

			boolean[] isNull = new boolean[types.length];

			for (int i = 0; i < types.length; i++)
			{
				isNull[i] = aBuffer.readBit() == 1;
			}

			aBuffer.align();

			int i = 0;
			for (FieldDescriptor fieldType : types)
			{
				if (!isNull[i++])
				{
					Object value = FieldReader.readField(fieldType, aBuffer);

					resultSet.add(fieldType, value);
				}
			}

			Log.dec();

			return resultSet;
		}
		catch (IOException | ClassNotFoundException e)
		{
			throw new DatabaseException(e);
		}
	}
}

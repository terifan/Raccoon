package org.terifan.raccoon.serialization;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
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


	public ByteArrayBuffer marshal(ByteArrayBuffer aBuffer, Object aObject, int aCategory)
	{
		return marshalImpl(aBuffer, aObject, mEntityDescriptor.getFields(aCategory));
	}


	public void unmarshal(ByteArrayBuffer aBuffer, Object aObject, int aCategory)
	{
		unmarshalImpl(aBuffer, aObject, mEntityDescriptor.getFields(aCategory));
	}


	public ResultSet unmarshal(ByteArrayBuffer aBuffer, ResultSet aResultSet, int aCategory)
	{
		return unmarshalImpl(aBuffer, aResultSet, mEntityDescriptor.getFields(aCategory));
	}


	private ByteArrayBuffer marshalImpl(ByteArrayBuffer aBuffer, Object aObject, ArrayList<FieldDescriptor> aTypes)
	{
		try
		{
			Log.v("marshal entity fields %s", aTypes);
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

			for (int i = 0; i < aTypes.size(); i++)
			{
				Object value = values.get(i);

				if (value != null)
				{
					FieldWriter.writeField(aTypes.get(i), value, aBuffer);
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


	private void unmarshalImpl(ByteArrayBuffer aBuffer, Object aObject, ArrayList<FieldDescriptor> aTypes)
	{
		try
		{
			Log.v("unmarshal entity fields");
			Log.inc();

			ArrayList<FieldDescriptor> readFields = new ArrayList<>();

			for (int i = 0; i < aTypes.size(); i++)
			{
				if (aBuffer.readBit() == 0)
				{
					readFields.add(aTypes.get(i));
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


	private ResultSet unmarshalImpl(ByteArrayBuffer aBuffer, ResultSet aResultSet, ArrayList<FieldDescriptor> aTypes)
	{
		try
		{
			Log.v("unmarshal entity fields");
			Log.inc();

			boolean[] isNull = new boolean[aTypes.size()];

			for (int i = 0; i < aTypes.size(); i++)
			{
				isNull[i] = aBuffer.readBit() == 1;
			}

			aBuffer.align();

			int i = 0;
			for (FieldDescriptor fieldType : aTypes)
			{
				if (!isNull[i++])
				{
					Object value = FieldReader.readField(fieldType, aBuffer);

					aResultSet.add(fieldType, value);
				}
			}

			Log.dec();

			return aResultSet;
		}
		catch (IOException | ClassNotFoundException e)
		{
			throw new DatabaseException(e);
		}
	}
}

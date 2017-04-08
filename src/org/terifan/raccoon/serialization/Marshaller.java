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


	public Marshaller(EntityDescriptor aTypeDeclarations)
	{
		mEntityDescriptor = aTypeDeclarations;
	}


	public ByteArrayBuffer marshal(ByteArrayBuffer aBuffer, Object aObject, int aCategory)
	{
		return marshalImpl(aBuffer, aObject, aCategory);
	}


	public void unmarshal(ByteArrayBuffer aBuffer, Object aObject, int aCategory)
	{
		unmarshalImpl(aBuffer, aObject, aCategory, false);
	}


	public ResultSet unmarshal(ByteArrayBuffer aBuffer, ResultSet aResultSet, int aCategory)
	{
		return unmarshalImpl(aBuffer, aResultSet, aCategory, true);
	}


	private ByteArrayBuffer marshalImpl(ByteArrayBuffer aBuffer, Object aObject, int aCategory)
	{
		try
		{
			Log.v("marshal entity fields %s", aCategory);
			Log.inc();

			ArrayList<Object> values = new ArrayList<>();
			ArrayList<FieldDescriptor> types = new ArrayList<>();

			for (FieldDescriptor fieldType : mEntityDescriptor.getFields())
			{
				if (isSelected(fieldType, aCategory))
				{
					Object value = fieldType.getField().get(aObject);

					if (fieldType.isNullable())
					{
						aBuffer.writeBit(value == null);
					}
					else if (value == null)
					{
						throw new IllegalStateException("Field is null when it cannot be: " + fieldType);
					}

					if (value != null)
					{
						types.add(fieldType);
						values.add(value);
					}
				}
			}

			aBuffer.align();

			for (int i = 0; i < values.size(); i++)
			{
				FieldWriter.writeField(types.get(i), values.get(i), aBuffer);
			}

			Log.dec();

			return aBuffer;
		}
		catch (IllegalAccessException | IOException e)
		{
			throw new DatabaseException(e);
		}
	}


	private void unmarshalImpl(ByteArrayBuffer aBuffer, Object aObject, int aCategory, boolean aIgnoreMissingClasses)
	{
		try
		{
			Log.v("unmarshal entity fields");
			Log.inc();

			ArrayList<FieldDescriptor> fields = new ArrayList<>();

			for (int i = 0; i < mEntityDescriptor.getFields().length; i++)
			{
				FieldDescriptor fieldType = mEntityDescriptor.getFields()[i];

				if (isSelected(fieldType, aCategory) && (!fieldType.isNullable() || aBuffer.readBit() == 0))
				{
					fields.add(fieldType);
				}
			}

			aBuffer.align();

			for (FieldDescriptor fieldDescriptor : fields)
			{
				Object value = FieldReader.readField(fieldDescriptor, aBuffer, aIgnoreMissingClasses);

				if (aObject != null)
				{
					Field field = fieldDescriptor.getField();

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


	private ResultSet unmarshalImpl(ByteArrayBuffer aBuffer, ResultSet aResultSet, int aCategory, boolean aIgnoreMissingClasses)
	{
		try
		{
			Log.v("unmarshal entity fields");
			Log.inc();

			ArrayList<FieldDescriptor> readFields = new ArrayList<>();

			for (int i = 0; i < mEntityDescriptor.getFields().length; i++)
			{
				FieldDescriptor fieldType = mEntityDescriptor.getFields()[i];

				if (isSelected(fieldType, aCategory) && (!fieldType.isNullable() || aBuffer.readBit() == 0))
				{
					readFields.add(fieldType);
				}
			}

			aBuffer.align();

			for (FieldDescriptor fieldType : readFields)
			{
				Object value = FieldReader.readField(fieldType, aBuffer, aIgnoreMissingClasses);

				aResultSet.add(fieldType, value);
			}

			Log.dec();

			return aResultSet;
		}
		catch (IOException | ClassNotFoundException e)
		{
			throw new DatabaseException(e);
		}
	}


	protected static boolean isSelected(FieldDescriptor aFieldType, int aCategory)
	{
		return (aFieldType.getCategory() & aCategory) != 0;
	}
}

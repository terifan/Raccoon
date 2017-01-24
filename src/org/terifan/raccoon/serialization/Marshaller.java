package org.terifan.raccoon.serialization;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import org.terifan.raccoon.DatabaseException;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;
import org.terifan.raccoon.util.ResultSet;


public class Marshaller
{
	private final static HashMap<EntityDescriptor,Marshaller> mMarshallers = new HashMap<>();

	private EntityDescriptor mEntityDescriptor;


	private Marshaller(EntityDescriptor aTypeDeclarations)
	{
		mEntityDescriptor = aTypeDeclarations;
	}


	public static synchronized Marshaller getInstance(EntityDescriptor aTypeDeclarations)
	{
		Marshaller instance = mMarshallers.get(aTypeDeclarations);

		if (instance == null)
		{
			instance = new Marshaller(aTypeDeclarations);
			mMarshallers.put(aTypeDeclarations, instance);
		}

		return instance;
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


	private ByteArrayBuffer marshalImpl(ByteArrayBuffer aBuffer, Object aObject, FieldType[] types)
	{
		try
		{
			Log.v("marshal entity fields %s", Arrays.toString(types));
			Log.inc();

			for (FieldType fieldType : types)
			{
				aBuffer.writeBit(mEntityDescriptor.getField(fieldType).get(aObject) == null);
			}

			aBuffer.align();

			for (FieldType fieldType : types)
			{
				Object value = mEntityDescriptor.getField(fieldType).get(aObject);

				if (value != null)
				{
					FieldWriter.writeField(fieldType, value, aBuffer);
				}
			}

//			Log.hexDump(new ByteArrayBuffer(aBuffer.array()).capacity(aBuffer.position()).crop().array());

			Log.dec();

			return aBuffer;
		}
		catch (IllegalAccessException | IOException e)
		{
			throw new DatabaseException(e);
		}
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


	private void unmarshalImpl(ByteArrayBuffer aBuffer, Object aObject, FieldType[] types)
	{
		try
		{
			Log.v("unmarshal entity fields");
			Log.inc();

			boolean[] isNull = new boolean[types.length];

			for (int i = 0; i < types.length; i++)
			{
				isNull[i] = aBuffer.readBit() == 1;
			}

			aBuffer.align();

			int i = 0;
			for (FieldType fieldType : types)
			{
				if (!isNull[i++])
				{
					Object value = FieldReader.readField(fieldType, aBuffer);

					if (aObject != null)
					{
						Field field = mEntityDescriptor.getField(fieldType);

						if (field != null)
						{
							field.set(aObject, value);
						}
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


	private ResultSet unmarshalImpl(ByteArrayBuffer aBuffer, FieldType[] types)
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
			for (FieldType fieldType : types)
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


	public Marshaller xxxxxxxxxxxxx(Class aType)
	{
		if (aType != null) mEntityDescriptor.setType(aType);
		return this;
	}
}
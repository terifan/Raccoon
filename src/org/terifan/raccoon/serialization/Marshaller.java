package org.terifan.raccoon.serialization;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.terifan.raccoon.DatabaseException;
import static org.terifan.raccoon.serialization.FieldCategory.DISCRIMINATOR;
import static org.terifan.raccoon.serialization.FieldCategory.KEY;
import static org.terifan.raccoon.serialization.FieldCategory.VALUE;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;
import org.terifan.raccoon.util.ResultSet;


public class Marshaller
{
	private EntityDescriptor mEntityDescriptor;

	private final static List<FieldCategory> KEYS = Arrays.asList(KEY);
	private final static List<FieldCategory> DISCRIMINATORS = Arrays.asList(DISCRIMINATOR);
	private final static List<FieldCategory> VALUES = Arrays.asList(DISCRIMINATOR, VALUE);


	public Marshaller(EntityDescriptor aTypeDeclarations)
	{
		mEntityDescriptor = aTypeDeclarations;
	}


	public ByteArrayBuffer marshalKeys(ByteArrayBuffer aBuffer, Object aObject)
	{
		return marshalImpl(aBuffer, aObject, KEYS);
	}


	public ByteArrayBuffer marshalDiscriminators(ByteArrayBuffer aBuffer, Object aObject)
	{
		return marshalImpl(aBuffer, aObject, DISCRIMINATORS);
	}


	public ByteArrayBuffer marshalValues(ByteArrayBuffer aBuffer, Object aObject)
	{
		return marshalImpl(aBuffer, aObject, VALUES);
	}


	private ByteArrayBuffer marshalImpl(ByteArrayBuffer aBuffer, Object aObject, Collection<FieldCategory> aFieldCategories)
	{
		try
		{
			Log.v("marshal entity fields %s", aFieldCategories);
			Log.inc();

			FieldType[] types = mEntityDescriptor.getTypes();

			for (FieldType fieldType : types)
			{
				if (aFieldCategories.contains(fieldType.getCategory()))
				{
					aBuffer.writeBit(fieldType.getField().get(aObject) == null);
				}
			}

			aBuffer.align();

			for (FieldType fieldType : types)
			{
				if (aFieldCategories.contains(fieldType.getCategory()))
				{
					Object value = fieldType.getField().get(aObject);

					if (value != null)
					{
						FieldWriter.writeField(fieldType, value, aBuffer);
					}
				}
			}

//			Log.hexDump(new ByteArrayBuffer(aBuffer.array()).capacity(aBuffer.position()).crop().array());

			Log.dec();

			return aBuffer;
		}
		catch (IllegalAccessException e)
		{
			throw new DatabaseException(e);
		}
	}


	public void unmarshalKeys(ByteArrayBuffer aBuffer, Object aObject)
	{
		unmarshalImpl(aBuffer, aObject, KEYS);
	}


	public void unmarshalDiscriminators(ByteArrayBuffer aBuffer, Object aObject)
	{
		unmarshalImpl(aBuffer, aObject, DISCRIMINATORS);
	}


	public void unmarshalValues(ByteArrayBuffer aBuffer, Object aObject)
	{
		unmarshalImpl(aBuffer, aObject, VALUES);
	}


	private void unmarshalImpl(ByteArrayBuffer aBuffer, Object aObject, Collection<FieldCategory> aFieldCategories)
	{
		try
		{
			Log.v("unmarshal entity fields");
			Log.inc();

			FieldType[] types = mEntityDescriptor.getTypes();

			boolean[] isNull = new boolean[types.length];

			int i = 0;
			for (FieldType fieldType : types)
			{
				if (aFieldCategories.contains(fieldType.getCategory()))
				{
					isNull[i++] = aBuffer.readBit() == 1;
				}
			}

			aBuffer.align();

			i = 0;
			for (FieldType fieldType : types)
			{
				if (aFieldCategories.contains(fieldType.getCategory()))
				{
					if (!isNull[i++])
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


	public ResultSet unmarshalKeys(ByteArrayBuffer aBuffer)
	{
		return unmarshalImpl(aBuffer, KEYS);
	}


	public ResultSet unmarshalDiscriminators(ByteArrayBuffer aBuffer)
	{
		return unmarshalImpl(aBuffer, DISCRIMINATORS);
	}


	public ResultSet unmarshalValues(ByteArrayBuffer aBuffer)
	{
		return unmarshalImpl(aBuffer, VALUES);
	}


	private ResultSet unmarshalImpl(ByteArrayBuffer aBuffer, Collection<FieldCategory> aFieldCategories)
	{
		Log.v("unmarshal entity fields");
		Log.inc();

		ResultSet resultSet = new ResultSet();
		FieldType[] types = mEntityDescriptor.getTypes();

		boolean[] isNull = new boolean[types.length];

		int i = 0;
		for (FieldType fieldType : types)
		{
			if (aFieldCategories.contains(fieldType.getCategory()))
			{
				isNull[i++] = aBuffer.readBit() == 1;
			}
		}

		aBuffer.align();

		i = 0;

		for (FieldType fieldType : types)
		{
			if (aFieldCategories.contains(fieldType.getCategory()))
			{
				if (!isNull[i++])
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
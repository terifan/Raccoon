package org.terifan.raccoon.serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.util.Collection;
import org.terifan.raccoon.DatabaseException;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;
import tests._ArrayK1;
import tests._BigObject1K;


public class Marshaller
{
	private TableDescriptor mTableDescriptor;


	public Marshaller(TableDescriptor aTypeDeclarations)
	{
		mTableDescriptor = aTypeDeclarations;
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

			for (FieldType fieldType : mTableDescriptor.getTypes().values())
			{
				if (aFieldCategories.contains(fieldType.getCategory()))
				{
					Field field = fieldType.getField();

					if (field == null)
					{
						throw new DatabaseException("");
					}

					Object value = field.get(aObject);

					if (value != null)
					{
						aBuffer.writeVar32(fieldType.getIndex());

						FieldWriter.writeField(fieldType, value, aBuffer);
					}
				}
			}

			aBuffer.writeVar32(-1);

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

			for (int index; (index = aBuffer.readVar32()) != -1;)
			{
				FieldType fieldType = mTableDescriptor.getTypes().get(index);

				Object value = FieldReader.readField(fieldType, aBuffer);

				if (aObject != null && aFieldCategories.contains(fieldType.getCategory()))
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

			Log.dec();
		}
		catch (Exception e)
		{
			throw new DatabaseException("Failed to reconstruct entity: " + (aObject == null ? null : aObject.getClass()), e);
		}
	}


	public static void main(String... args)
	{
		try
		{
			byte[] formatData;
			byte[] entryData;

			{
				TableDescriptor td = new TableDescriptor(_BigObject1K.class);
//				TableDescriptor td = new TableDescriptor(_ArrayK1.class);
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				try (ObjectOutputStream oos = new ObjectOutputStream(baos))
				{
					td.writeExternal(oos);
				}

				formatData = baos.toByteArray();

				Marshaller marshaller = new Marshaller(td);

//				Object object = new _ArrayK1(new byte[1], new byte[]{1,2,3}, new byte[][]{{1,2,3},{4,5,6}}, new byte[][][]{{{1,2,3},{4,5,6}},{{7,8,9},{10,11,12}}});
				Object object = new _BigObject1K().random();
//				Object object = new _Number1K2D(15, "red", 12, "apple");

				ByteArrayBuffer buffer = new ByteArrayBuffer(16);
				marshaller.marshal(buffer, object, FieldCategoryFilter.ALL);

				entryData = buffer.trim().array();
			}

			Log.hexDump(entryData);

			TableDescriptor td = new TableDescriptor();
			td.readExternal(new ObjectInputStream(new ByteArrayInputStream(formatData)));
			td.mapFields(Class.forName(td.getName()));

			Marshaller marshaller = new Marshaller(td);

			Object object = Class.forName(td.getName()).newInstance();
			marshaller.unmarshal(entryData, object, FieldCategoryFilter.ALL);
			Log.out.println(object);

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			try (ObjectOutputStream oos = new ObjectOutputStream(baos))
			{
				oos.writeObject(object);
			}
			Log.hexDump(baos.toByteArray());
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}
}
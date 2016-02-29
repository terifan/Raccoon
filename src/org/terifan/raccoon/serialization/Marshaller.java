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
import tests._BigObject1K;
import tests._Number1K2D;


public class Marshaller
{
	private TableDescriptor mTypeDeclarations;


	public Marshaller(TableDescriptor aTypeDeclarations)
	{
		mTypeDeclarations = aTypeDeclarations;
	}


	public ByteArrayBuffer marshal(ByteArrayBuffer aBuffer, Object aObject, Collection<FieldCategory> aFieldCategories)
	{
		try
		{
			Log.v("marshal entity fields %s", aFieldCategories);
			Log.inc();

			for (FieldType fieldType : mTypeDeclarations.getTypes())
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
				FieldType fieldType = mTypeDeclarations.getTypes()[index];

				Object value = FieldReader.readField(fieldType, aBuffer);

				if (aObject != null && fieldType.getField() != null && aFieldCategories.contains(fieldType.getCategory()))
				{
					fieldType.getField().set(aObject, value);
				}
			}

			Log.dec();
		}
		catch (Exception e)
		{
			throw new DatabaseException("Failed to reconstruct entity: " + (aObject == null ? null : aObject.getClass()), e);
		}
	}


//	public HashMap<String, Object> unmarshal(Entry aEntry) throws IOException
//	{
//		Log.v("unmarshal entity");
//		Log.inc();
//
//		HashMap<String,Object> map = new HashMap<>();
//
//		ByteArrayBuffer buffer = new ByteArrayBuffer(aEntry.getKey());
//
//		for (FieldType fieldType : mTypeDeclarations.getTypes())
//		{
//			if (fieldType.getCategory() == FieldCategory.KEY)
//			{
//				map.put(fieldType.getName(), FieldReader.readField(fieldType, buffer, null));
//			}
//		}
//
//		buffer = aEntry.x();
//
//		for (FieldType fieldType : mTypeDeclarations.getTypes())
//		{
//			if (fieldType.getCategory() == FieldCategory.VALUE || fieldType.getCategory() == FieldCategory.DISCRIMINATOR)
//			{
//				map.put(fieldType.getName(), FieldReader.readField(fieldType, buffer, null));
//			}
//		}
//
//		Log.dec();
//
//		return map;
//	}
	
	
	public static void main(String... args)
	{
		try
		{
			byte[] formatData;
			byte[] entryData;

			{
				TableDescriptor td = new TableDescriptor(_BigObject1K.class);
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				try (ObjectOutputStream oos = new ObjectOutputStream(baos))
				{
					td.writeExternal(oos);
				}

				formatData = baos.toByteArray();

				Marshaller marshaller = new Marshaller(td);

				ByteArrayBuffer buffer = new ByteArrayBuffer(16);
				marshaller.marshal(buffer, new _BigObject1K().random(), FieldCategoryFilter.ALL);
//				marshaller.marshal(buffer, new _Number1K2D(15, "red", 12, "apple"), FieldCategoryFilter.ALL);
	
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
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}
}
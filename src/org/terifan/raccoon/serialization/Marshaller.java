package org.terifan.raccoon.serialization;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.LinkedHashMap;
import org.terifan.raccoon.DatabaseException;
import org.terifan.raccoon.Entry;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;


public class Marshaller
{
	

//	private LinkedHashMap<String, Field> mFields;
//	private TypeDeclarations mTypeDeclarations;
//
//
//	public Marshaller(TypeDeclarations aTypeDeclarations)
//	{
//		mTypeDeclarations = aTypeDeclarations;
//	}
//
//
//	public Marshaller(Class aType)
//	{
//		if (aType != null)
//		{
//			loadFields(aType);
//		}
//
//		Field[] keys = mFields.values().toArray(new Field[mFields.size()]);
//		mTypeDeclarations = new TypeDeclarations(aType, keys);
//	}
//
//
//	@Deprecated
//	public byte[] marshal(Object aObject, FieldCategory aFieldCategory)
//	{
//		return marshal(new ByteArrayBuffer(16), aObject, aFieldCategory).trim().array();
//	}
//
//
//	public ByteArrayBuffer marshal(ByteArrayBuffer aBuffer, Object aObject, FieldCategory aFieldCategory)
//	{
//		if (aObject != null && mFields == null)
//		{
//			loadFields(aObject.getClass());
//		}
//
//		try
//		{
//			Log.v("marshal entity fields %s", aFieldCategory);
//			Log.inc();
//
//			for (FieldType fieldType : mTypeDeclarations.getTypes())
//			{
//				if (fieldType.getCategory() == aFieldCategory || aFieldCategory == FieldCategory.DISCRIMINATOR_AND_VALUES && (fieldType.getCategory() == FieldCategory.VALUE || fieldType.getCategory() == FieldCategory.DISCRIMINATOR))
//				{
//					Field field = mFields.get(fieldType.getName());
//					if (field == null)
//					{
//						throw new DatabaseException("Field not found: " + fieldType.getName() + ", " + mFields.keySet());
//					}
//
//					Object value = field.get(aObject);
//					FieldWriter.writeField(fieldType, aBuffer, value);
//				}
//			}
//
//			aBuffer.align();
//
//			Log.dec();
//
//			return aBuffer;
//		}
//		catch (IllegalAccessException e)
//		{
//			throw new DatabaseException(e);
//		}
//	}
//
//
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
//
//
//	public HashMap<String, Object> unmarshalDISCRIMINATORS(byte[] aBuffer) throws IOException
//	{
//		Log.v("unmarshal entity");
//		Log.inc();
//
//		HashMap<String,Object> map = new HashMap<>();
//
//		ByteArrayBuffer buffer = new ByteArrayBuffer(aBuffer);
//
//		for (FieldType fieldType : mTypeDeclarations.getTypes())
//		{
//			if (fieldType.getCategory() == FieldCategory.DISCRIMINATOR)
//			{
//				map.put(fieldType.getName(), FieldReader.readField(fieldType, buffer, null));
//			}
//		}
//
//		Log.dec();
//
//		return map;
//	}
//
//
//	public void unmarshal(byte[] aBuffer, Object aOutputObject, FieldCategory aFieldCategory)
//	{
//		unmarshal(new ByteArrayBuffer(aBuffer), aOutputObject, aFieldCategory);
//	}
//
//
//	public void unmarshal(ByteArrayBuffer aBuffer, Object aOutputObject, FieldCategory aFieldCategory)
//	{
//		if (aOutputObject != null && mFields == null)
//		{
//			loadFields(aOutputObject.getClass());
//		}
//
//		try
//		{
//			Log.v("unmarshal entity");
//			Log.inc();
//
//			for (FieldType fieldType : mTypeDeclarations.getTypes())
//			{
//				if (fieldType.getCategory() == aFieldCategory || aFieldCategory == FieldCategory.DISCRIMINATOR_AND_VALUES && (fieldType.getCategory() == FieldCategory.VALUE || fieldType.getCategory() == FieldCategory.DISCRIMINATOR))
//				{
//					Field field = mFields.get(fieldType.getName());
//					Object value = FieldReader.readField(fieldType, aBuffer, field);
//
//					if (field != null && aOutputObject != null)
//					{
//						field.set(aOutputObject, value);
//					}
//				}
//			}
//
//			Log.dec();
//		}
//		catch (IOException | IllegalArgumentException | IllegalAccessException e)
//		{
//			throw new DatabaseException("Failed to reconstruct entity: " + (aOutputObject == null ? null : aOutputObject.getClass()), e);
//		}
//	}
//
//
//	public TypeDeclarations getTypeDeclarations()
//	{
//		return mTypeDeclarations;
//	}
//
//
//	private void loadFields(Class aType) throws SecurityException
//	{
//		mFields = new LinkedHashMap<>();
//
//		for (Field field : aType.getDeclaredFields())
//		{
//			if ((field.getModifiers() & (Modifier.TRANSIENT | Modifier.STATIC | Modifier.FINAL)) != 0)
//			{
//				continue;
//			}
//
//			field.setAccessible(true);
//
//			mFields.put(field.getName(), field);
//		}
//	}
}
package org.terifan.raccoon.serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.terifan.raccoon.DatabaseException;
import org.terifan.raccoon.util.Log;


public class Marshaller
{
	private HashMap<String, Field> mFields;
	private TypeDeclarations mTypeDeclarations;


	public Marshaller(Class aType)
	{
		mFields = new HashMap<>();

		if (aType != null)
		{
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

		mTypeDeclarations = new TypeDeclarations(aType, mFields);
	}


	public byte[] marshal(Object aObject, FieldCategory aFieldCategory)
	{
		try
		{
			Log.v("marshal entity fields " + aFieldCategory);
			Log.inc();

			ByteArrayOutputStream baos = new ByteArrayOutputStream();

			try (DataOutputStream out = new DataOutputStream(baos))
			{
				for (Entry<Integer, FieldType> entry : mTypeDeclarations.entrySet())
				{
					FieldType typeInfo = entry.getValue();

					if (typeInfo.category != aFieldCategory)
					{
						continue;
					}

					Field field = findField(typeInfo);
					Object value = field.get(aObject);

					if (value != null)
					{
						Integer index = entry.getKey();

						Log.v("encode "+index+" "+typeInfo);

						out.writeInt(index);

						if (typeInfo.array)
						{
							writeArray(typeInfo, value, 1, typeInfo.depth, out);
						}
						else if (List.class.isAssignableFrom(typeInfo.type) || Set.class.isAssignableFrom(typeInfo.type))
						{
							writeArray(typeInfo.componentType[0], ((Collection)value).toArray(), 1, typeInfo.componentType[0].depth + 1, out);
						}
						else if (Map.class.isAssignableFrom(typeInfo.type))
						{
							writeArray(typeInfo.componentType[0], ((Map)value).keySet().toArray(), 1, typeInfo.componentType[0].depth + 1, out);
							writeArray(typeInfo.componentType[1], ((Map)value).values().toArray(), 1, typeInfo.componentType[1].depth + 1, out);
						}
						else
						{
							writeValue(value, out);
						}
					}
				}
			}

			Log.dec();

			return baos.toByteArray();
		}
		catch (IOException | IllegalAccessException e)
		{
			throw new DatabaseException(e);
		}
	}


	private void writeArray(FieldType aTypeInfo, Object aArray, int aLevel, int aDepth, DataOutputStream aOutputStream) throws IOException, IllegalAccessException
	{
		assert aLevel <= aDepth : aLevel+" <= "+aDepth;

		int length = Array.getLength(aArray);

		if (aLevel == aDepth)
		{
			writeArrayContents(aTypeInfo, aArray, length, aOutputStream);
		}
		else
		{
			writeArrayHeader(aArray, length, aOutputStream);

			for (int i = 0; i < length; i++)
			{
				Object value = Array.get(aArray, i);

				if (value != null)
				{
					writeArray(aTypeInfo, value, aLevel + 1, aDepth, aOutputStream);
				}
			}
		}
	}


	private void writeArrayContents(FieldType aTypeInfo, Object aArray, int aLength, DataOutputStream aOutputStream) throws IOException, IllegalAccessException
	{
		if (aTypeInfo.primitive)
		{
			aOutputStream.writeInt(aLength);

			if (aTypeInfo.type == Byte.TYPE)
			{
				aOutputStream.write((byte[])aArray);
			}
			else
			{
				for (int i = 0; i < aLength; i++)
				{
					writeValue(Array.get(aArray, i), aOutputStream);
				}
			}
		}
		else
		{
			writeArrayHeader(aArray, aLength, aOutputStream);

			for (int i = 0; i < aLength; i++)
			{
				Object value = Array.get(aArray, i);
				if (value != null)
				{
					writeValue(value, aOutputStream);
				}
			}
		}
	}


	private void writeArrayHeader(Object aArray, int aLength, DataOutputStream aOutputStream) throws IOException
	{
		byte[] bitmap = null;

		for (int i = 0; i < aLength; i++)
		{
			if (Array.get(aArray, i) == null)
			{
				if (bitmap == null)
				{
					bitmap = new byte[(aLength + 7) / 8];
				}
				bitmap[i >> 3] |= 128 >> (i & 7);
			}
		}

		aOutputStream.writeInt(aLength);
		aOutputStream.writeBoolean(bitmap != null);

		if (bitmap != null)
		{
			aOutputStream.write(bitmap);
		}
	}


	private void writeValue(Object aValue, DataOutputStream aOutputStream) throws IOException
	{
		Class<?> type = aValue.getClass();

		if (type == Boolean.class)
		{
			aOutputStream.writeBoolean((Boolean)aValue);
		}
		else if (type == Byte.class)
		{
			aOutputStream.writeByte((Byte)aValue);
		}
		else if (type == Short.class)
		{
			aOutputStream.writeShort((Short)aValue);
		}
		else if (type == Character.class)
		{
			aOutputStream.writeChar((Character)aValue);
		}
		else if (type == Integer.class)
		{
			aOutputStream.writeInt((Integer)aValue);
		}
		else if (type == Long.class)
		{
			aOutputStream.writeLong((Long)aValue);
		}
		else if (type == Float.class)
		{
			aOutputStream.writeFloat((Float)aValue);
		}
		else if (type == Double.class)
		{
			aOutputStream.writeDouble((Double)aValue);
		}
		else if (type == String.class)
		{
			aOutputStream.writeUTF((String)aValue);
		}
		else if (Date.class.isAssignableFrom(type))
		{
			aOutputStream.writeLong(((Date)aValue).getTime());
		}
		else
		{
			throw new IllegalArgumentException("Unsupported: " + type);
		}
	}


	public void unmarshal(Object aObject, byte[] aBuffer)
	{
		if (aBuffer.length == 0)
		{
			return;
		}

		try
		{
			Log.v("unmarshal entity");
			Log.inc();

			try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(aBuffer)))
			{
				for (int counter = 0, fieldCount = mTypeDeclarations.size(), prevIndex = 0; counter < fieldCount; counter++)
				{
					int index = in.readInt();

					if (index == 0)
					{
						break;
					}

					if (index <= prevIndex || index > fieldCount)
					{
						Log.e("decoding field index " + index + ", previous " + prevIndex + ", limit " + fieldCount);

						throw new IOException("Stream corrupted");
					}

					FieldType typeInfo = mTypeDeclarations.get(index);
					Field field = findField(typeInfo);
					Object value;

					Log.v("decode "+index+" "+typeInfo);

					switch (typeInfo.code)
					{
						case 1:
							value = readValue(typeInfo.type, in);
							break;
						case 2:
							value = readArray(typeInfo, 1, typeInfo.depth, in);
							Log.inc();
							Log.v("decoded array: " + typeInfo.type+"["+Array.getLength(value)+"]");
							Log.dec();
							break;
						case 3:
							value = readCollection(field, aObject, typeInfo, in);
							break;
						case 4:
							value = readMap(field, aObject, typeInfo, in);
							break;
						default:
							throw new Error();
					}

					if (field != null)
					{
						field.set(aObject, value);
					}

					prevIndex = index;
				}
			}

			Log.dec();
		}
		catch (IOException | IllegalAccessException e)
		{
			throw new DatabaseException("Failed to reconstruct entity: " + aObject.getClass(), e);
		}
	}


	private Field findField(FieldType aTypeInfo)
	{
		return mFields.get(aTypeInfo.name);
	}


	private Object readMap(Field aField, Object aObject, FieldType aTypeInfo, DataInputStream aInputStream) throws IOException, IllegalAccessException
	{
		Map value = aField == null ? null : (Map)aField.get(aObject);

		if (value == null && !aTypeInfo.type.isInterface())
		{
			try
			{
				value = (Map)aTypeInfo.type.newInstance();
			}
			catch (InstantiationException e)
			{
			}
		}
		if (value == null)
		{
			value = new HashMap();
		}

		FieldType componentType0 = aTypeInfo.componentType[0];
		FieldType componentType1 = aTypeInfo.componentType[1];
		Object keys = readArray(componentType0, 1, componentType0.depth + 1, aInputStream);
		Object values = readArray(componentType1, 1, componentType1.depth + 1, aInputStream);

		for (int i = 0, sz = Array.getLength(keys); i < sz; i++)
		{
			value.put(Array.get(keys, i), Array.get(values, i));
		}

		return value;
	}


	private Object readCollection(Field aField, Object aObject, FieldType aTypeInfo, DataInputStream aInputStream) throws IOException, IllegalAccessException
	{
		Collection value = aField == null ? null : (Collection)aField.get(aObject);

		if (value == null && !aTypeInfo.type.isInterface())
		{
			try
			{
				value = (Collection)aTypeInfo.type.newInstance();
			}
			catch (InstantiationException e)
			{
			}
		}
		if (value == null)
		{
			if (List.class.isAssignableFrom(aTypeInfo.type))
			{
				value = new ArrayList();
			}
			else
			{
				value = new HashSet();
			}
		}

		FieldType componentType = aTypeInfo.componentType[0];
		Object values = readArray(componentType, 1, componentType.depth + 1, aInputStream);

		for (int i = 0, sz = Array.getLength(values); i < sz; i++)
		{
			value.add(Array.get(values, i));
		}

		return value;
	}


	private Object readValue(Class type, DataInputStream aInputStream) throws IOException, IllegalAccessException
	{
		if (type == Boolean.class || type == Boolean.TYPE)
		{
			return aInputStream.readBoolean();
		}
		if (type == Byte.class || type == Byte.TYPE)
		{
			return aInputStream.readByte();
		}
		if (type == Short.class || type == Short.TYPE)
		{
			return aInputStream.readShort();
		}
		if (type == Character.class || type == Character.TYPE)
		{
			return aInputStream.readChar();
		}
		if (type == Integer.class || type == Integer.TYPE)
		{
			return aInputStream.readInt();
		}
		if (type == Long.class || type == Long.TYPE)
		{
			return aInputStream.readLong();
		}
		if (type == Float.class || type == Float.TYPE)
		{
			return aInputStream.readFloat();
		}
		if (type == Double.class || type == Double.TYPE)
		{
			return aInputStream.readDouble();
		}
		if (type == String.class)
		{
			return aInputStream.readUTF();
		}
		if (type == Date.class)
		{
			return new Date(aInputStream.readLong());
		}

		throw new IllegalArgumentException("Unsupported type: " + type);
	}


	private Object readArray(FieldType aTypeInfo, int aLevel, int aDepth, DataInputStream aInputStream) throws IOException, IllegalAccessException
	{
		int length = aInputStream.readInt();
		boolean hasNulls = aInputStream.readBoolean();
		byte[] bitmap = null;

		if (hasNulls)
		{
			bitmap = new byte[(length + 7) / 8];
			aInputStream.readFully(bitmap);
		}

		int[] dims = new int[aDepth - aLevel + 1];
		dims[0] = length;

		Object array = Array.newInstance(aTypeInfo.type, dims);

		if (aLevel == aDepth && aTypeInfo.type == Byte.TYPE)
		{
			aInputStream.readFully((byte[])array);
		}
		else
		{
			for (int i = 0; i < length; i++)
			{
				Object value = null;
				if (!hasNulls || ((bitmap[i >> 3] & (128 >> (i & 7))) == 0))
				{
					if (aLevel == aDepth)
					{
						value = readValue(aTypeInfo.type, aInputStream);
					}
					else
					{
						value = readArray(aTypeInfo, aLevel + 1, aDepth, aInputStream);
					}
				}
				Array.set(array, i, value);
			}
		}

		return array;
	}
}
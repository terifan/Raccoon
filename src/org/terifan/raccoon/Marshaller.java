package org.terifan.raccoon;

import org.terifan.raccoon.util.Logger;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import org.terifan.raccoon.util.ByteArray;


public class Marshaller
{
	private final static Class[] PRIMITIVE_TYPES = new Class[]{Boolean.TYPE, Byte.TYPE, Short.TYPE, Character.TYPE, Integer.TYPE, Long.TYPE, Float.TYPE, Double.TYPE};

	private HashMap<String, Field> mFields;
	private TreeMap<Integer,FieldType> mTypeDeclarations;
	private MarshallerListener mListener;
	private Logger mLogger;
	private boolean mHasDiscriminiators;

	public enum Category
	{
		KEY,
		DISCRIMINATOR,
		VALUE;


		private static Category classify(Field aField)
		{
			if (aField.getAnnotation(Key.class) != null)
			{
				return Category.KEY;
			}
			if (aField.getAnnotation(Discriminator.class) != null)
			{
				return Category.DISCRIMINATOR;
			}
			return Category.VALUE;
		}
	}


	public Marshaller(Class aType)
	{
		this(aType, null);

		createTypeDeclarations();
	}


	public Marshaller(Class aType, Object aTypeDeclarations)
	{
		mFields = new HashMap<>();
		mTypeDeclarations = (TreeMap<Integer,FieldType>)aTypeDeclarations;
		mLogger = new Logger(aType == null ? "" : aType.getSimpleName());

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
	}


	public boolean hasDiscriminators()
	{
		return mHasDiscriminiators;
	}


	public void setListener(MarshallerListener aListener)
	{
		mListener = aListener;
	}


	private Object createTypeDeclarations()
	{
		mTypeDeclarations = new TreeMap<>();

		int index = 1;

		for (Field field : mFields.values())
		{
			FieldType typeInfo = new FieldType();
			typeInfo.name = field.getName();
			typeInfo.type = field.getType();
			typeInfo.array = typeInfo.type.isArray();
			while (typeInfo.type.isArray())
			{
				typeInfo.type = typeInfo.type.getComponentType();
				typeInfo.depth++;
			}
			typeInfo.primitive = typeInfo.type.isPrimitive();
			typeInfo.category = Category.classify(field);

			mTypeDeclarations.put(index, typeInfo);

			if (typeInfo.array)
			{
				typeInfo.code = 2;
			}
			else if (List.class.isAssignableFrom(typeInfo.type) || Set.class.isAssignableFrom(typeInfo.type))
			{
				getGenericType(field, typeInfo, 0);
				typeInfo.code = 3;
			}
			else if (Map.class.isAssignableFrom(typeInfo.type))
			{
				getGenericType(field, typeInfo, 0);
				getGenericType(field, typeInfo, 1);
				typeInfo.code = 4;
			}
			else if (typeInfo.type.isPrimitive() || isValidType(typeInfo.type))
			{
				typeInfo.code = 1;
			}
			else
			{
				throw new IllegalArgumentException("Unsupported type: " + field);
			}

			if (typeInfo.category == Category.DISCRIMINATOR)
			{
				mHasDiscriminiators = true;
			}

			mLogger.i("type found: "+index+" "+typeInfo);

			index++;
		}

		return mTypeDeclarations;
	}


	TreeMap<Integer, FieldType> getTypeDeclarations()
	{
		return mTypeDeclarations;
	}


	public byte[] marshal(Object aObject, Category aFieldCategory)
	{
		if (mTypeDeclarations == null)
		{
			throw new IllegalArgumentException("A TypeDeclarations must be created or loaded");
		}

		try
		{
			mLogger.inc("marshal entity");

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

					Integer index = entry.getKey();
					Field field = mFields.get(typeInfo.name);
					Object value = field.get(aObject);

					if (value == null)
					{
						continue;
					}

					mLogger.i("encode "+index+" "+typeInfo);

					ByteArray.putVarLong(out, index);

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

			mLogger.dec();

			return baos.toByteArray();
		}
		catch (IOException | IllegalAccessException e)
		{
			throw new DatabaseException(e);
		}
	}


	private void getGenericType(Field aField, FieldType aTypeInfo, int aIndex)
	{
		if (!(aField.getGenericType() instanceof ParameterizedType))
		{
			throw new IllegalArgumentException("Generic type must be parameterized: " + aField);
		}

		if (aTypeInfo.componentType == null)
		{
			aTypeInfo.componentType = new FieldType[2];
		}

		FieldType componentType = new FieldType();
		aTypeInfo.componentType[aIndex] = componentType;

		String typeName = ((ParameterizedType)aField.getGenericType()).getActualTypeArguments()[aIndex].getTypeName();

		while (typeName.endsWith("[]"))
		{
			typeName = typeName.substring(0, typeName.length() - 2);
			componentType.depth++;
			componentType.array = true;
		}

		for (Class basicType : PRIMITIVE_TYPES)
		{
			if (typeName.equals(basicType.getSimpleName()))
			{
				componentType.primitive = true;
				componentType.type = basicType;

				return;
			}
		}

		Class<?> type;

		try
		{
			type = Class.forName(typeName);
		}
		catch (ClassNotFoundException e)
		{
			throw new DatabaseException(e);
		}

		if (!isValidType(type))
		{
			throw new IllegalArgumentException("Unsupported type: " + type);
		}

		componentType.type = type;
	}


	private boolean isValidType(Class aType)
	{
		return Number.class.isAssignableFrom(aType) || aType == String.class || aType == Character.class || aType == Boolean.class || aType == Date.class;
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
			ByteArray.putVarLong(aOutputStream, aLength);

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
		byte[] bitmap = new byte[(aLength + 7) / 8];

		int nulls = 0;
		for (int i = 0; i < aLength; i++)
		{
			if (Array.get(aArray, i) == null)
			{
				bitmap[i >> 3] |= 128 >> (i & 7);
				nulls = 1;
			}
		}

		ByteArray.putVarLong(aOutputStream, (aLength << 1) | nulls);

		if (nulls == 1)
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
			ByteArray.putVarLong(aOutputStream, ByteArray.encodeZigZag32((Short)aValue));
		}
		else if (type == Character.class)
		{
			ByteArray.putVarLong(aOutputStream, (Character)aValue);
		}
		else if (type == Integer.class)
		{
			ByteArray.putVarLong(aOutputStream, ByteArray.encodeZigZag32((Integer)aValue));
		}
		else if (type == Long.class)
		{
			ByteArray.putVarLong(aOutputStream, ByteArray.encodeZigZag64((Long)aValue));
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
			String s = (String)aValue;
			ByteArray.putVarLong(aOutputStream, s.length());
			aOutputStream.write(ByteArray.encodeUTF8(s));
		}
		else if (Date.class.isAssignableFrom(type))
		{
			ByteArray.putVarLong(aOutputStream, ((Date)aValue).getTime());
		}
//		else if (Bundle.class.isAssignableFrom(type))
//		{
//			new BinaryEncoder().marshal((Bundle)aValue, aOutputStream);
//		}
		else
		{
			throw new IllegalArgumentException("Unsupported: " + type);
		}
	}


	static class FieldType implements Serializable
	{
		private final static long serialVersionUID = 1L;

		Category category;
		String name;
		Class type;
		boolean array;
		boolean primitive;
		int depth;
		int code;
		FieldType[] componentType;


		@Override
		public String toString()
		{
			StringBuilder s = new StringBuilder();
			s.append(category);
			s.append(" ");
			s.append("("+code+")");
			s.append(" ");
			s.append(type.getSimpleName());
			for (int i = 0; i < depth; i++)
			{
				s.append("[]");
			}
			if (componentType != null)
			{
				s.append("<");
				s.append(componentType[0].type.getSimpleName());
				if (componentType[0].array)
				{
					for (int i = 0; i < componentType[0].depth; i++)
					{
						s.append("[]");
					}
				}
				if (componentType[1] != null)
				{
					s.append("," + componentType[1].type.getSimpleName());
					if (componentType[1].array)
					{
						for (int i = 0; i < componentType[1].depth; i++)
						{
							s.append("[]");
						}
					}
				}
				s.append(">");
			}
			s.append(" " + name);
			return s.toString();
		}
	}


	public void unmarshal(Object aObject, byte[] aBuffer)
	{
		try
		{
			mLogger.inc("unmarshal entity");

			try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(aBuffer)))
			{
				for (int counter = 0, fieldCount = mTypeDeclarations.size(), prevIndex = 0; counter < fieldCount; counter++)
				{
					int index = (int)ByteArray.getVarLong(in);

					if (index == 0)
					{
						break;
					}

					if (index <= prevIndex || index > fieldCount)
					{
						mLogger.e("decoding field index " + index + ", previous " + prevIndex + ", limit " + fieldCount);

						throw new IOException("Stream corrupted");
					}

					FieldType typeInfo = mTypeDeclarations.get(index);
					Field field = mFields.get(typeInfo.name);
					Object value;

					mLogger.i("decode "+index+" "+typeInfo);

					switch (typeInfo.code)
					{
						case 1:
							value = readValue(typeInfo.type, in);
							break;
						case 2:
							value = readArray(typeInfo, 1, typeInfo.depth, in);
							mLogger.inc().d("decoded array: " + typeInfo.type+"["+Array.getLength(value)+"]").dec();
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
					if (mListener != null)
					{
						mListener.valueDecoded(aObject, field, typeInfo.name, value);
					}

					prevIndex = index;
				}
			}

			mLogger.dec();
		}
		catch (IOException | IllegalAccessException e)
		{
			throw new DatabaseException("Failed to reconstruct entity: " + aObject.getClass(), e);
		}
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
			return (short)ByteArray.decodeZigZag32((int)ByteArray.getVarLong(aInputStream));
		}
		if (type == Character.class || type == Character.TYPE)
		{
			return (char)ByteArray.getVarLong(aInputStream);
		}
		if (type == Integer.class || type == Integer.TYPE)
		{
			return ByteArray.decodeZigZag32((int)ByteArray.getVarLong(aInputStream));
		}
		if (type == Long.class || type == Long.TYPE)
		{
			return ByteArray.decodeZigZag64(ByteArray.getVarLong(aInputStream));
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
			return ByteArray.decodeUTF8(aInputStream, (int)ByteArray.getVarLong(aInputStream));
		}
		if (type == Date.class)
		{
			return new Date(ByteArray.getVarLong(aInputStream));
		}
//		if (type == Bundle.class)
//		{
//			return new BinaryDecoder().unmarshal(aInputStream);
//		}

		throw new IllegalArgumentException("Unsupported type: " + type);
	}


	private Object readArray(FieldType aTypeInfo, int aLevel, int aDepth, DataInputStream aInputStream) throws IOException, IllegalAccessException
	{
		int length = (int)ByteArray.getVarLong(aInputStream);

		boolean hasNulls = false;
		byte[] bitmap = null;

		if (aLevel < aDepth || !aTypeInfo.primitive)
		{
			hasNulls = (length & 1) == 1;
			length >>= 1;

			if (hasNulls)
			{
				bitmap = new byte[(length + 7) / 8];
				aInputStream.readFully(bitmap);
			}
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


	interface MarshallerListener
	{
		void valueDecoded(Object aObject, Field aField, String aFieldName, Object aValue);
	}
}
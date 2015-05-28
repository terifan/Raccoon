package org.terifan.raccoon.serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.terifan.raccoon.DatabaseException;
import org.terifan.raccoon.Discriminator;
import org.terifan.raccoon.Key;
import org.terifan.raccoon.util.Log;


class TypeDeclarations
{
	private final static HashMap<String,Class> PRIMITIVE_TYPES;

	static
	{
		PRIMITIVE_TYPES = new HashMap<>();
		PRIMITIVE_TYPES.put(Boolean.TYPE.getSimpleName(), Boolean.TYPE);
		PRIMITIVE_TYPES.put(Byte.TYPE.getSimpleName(), Byte.TYPE);
		PRIMITIVE_TYPES.put(Short.TYPE.getSimpleName(), Short.TYPE);
		PRIMITIVE_TYPES.put(Character.TYPE.getSimpleName(), Character.TYPE);
		PRIMITIVE_TYPES.put(Integer.TYPE.getSimpleName(), Integer.TYPE);
		PRIMITIVE_TYPES.put(Long.TYPE.getSimpleName(), Long.TYPE);
		PRIMITIVE_TYPES.put(Float.TYPE.getSimpleName(), Float.TYPE);
		PRIMITIVE_TYPES.put(Double.TYPE.getSimpleName(), Double.TYPE);
	}

	private ArrayList<FieldType> mTypes;


	public TypeDeclarations(byte[] aTypeBlob) throws IOException
	{
		try (ObjectInputStream oos = new ObjectInputStream(new ByteArrayInputStream(aTypeBlob)))
		{
			mTypes = (ArrayList<FieldType>)oos.readObject();
		}
		catch (ClassNotFoundException e)
		{
			throw new IOException(e);
		}
	}


	public TypeDeclarations(Class aType, HashMap<String, Field> mFields)
	{
		Log.v("create type declarations for " + aType);
		Log.inc();

		mTypes = new ArrayList<>();

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
			typeInfo.category = classify(field);

			mTypes.add(typeInfo);

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

			Log.v("type found: " + typeInfo);
		}

		Log.dec();
	}


	public FieldType get(int aIndex)
	{
		return mTypes.get(aIndex);
	}


	public int size()
	{
		return mTypes.size();
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

		Class primitiveType = PRIMITIVE_TYPES.get(typeName);
		if (primitiveType != null)
		{
			componentType.primitive = true;
			componentType.type = primitiveType;
			return;
		}

		try
		{
			Class<?> type = Class.forName(typeName);

			if (!isValidType(type))
			{
				throw new IllegalArgumentException("Unsupported type: " + type);
			}

			componentType.type = type;
		}
		catch (ClassNotFoundException e)
		{
			throw new DatabaseException(e);
		}
	}


	private boolean isValidType(Class aType)
	{
		return Number.class.isAssignableFrom(aType) || aType == String.class || aType == Character.class || aType == Boolean.class || aType == Date.class;
	}


	private FieldCategory classify(Field aField)
	{
		if (aField.getAnnotation(Key.class) != null)
		{
			return FieldCategory.KEY;
		}
		if (aField.getAnnotation(Discriminator.class) != null)
		{
			return FieldCategory.DISCRIMINATOR;
		}
		return FieldCategory.VALUE;
	}


	byte[] marshal() throws IOException
	{
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try (ObjectOutputStream oos = new ObjectOutputStream(baos))
		{
			oos.writeObject(mTypes);
		}
		return baos.toByteArray();
	}
}
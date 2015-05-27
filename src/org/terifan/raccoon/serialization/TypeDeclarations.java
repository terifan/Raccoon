package org.terifan.raccoon.serialization;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.terifan.raccoon.DatabaseException;
import org.terifan.raccoon.Discriminator;
import org.terifan.raccoon.Key;
import org.terifan.raccoon.util.Log;


class TypeDeclarations
{
	private final static Class[] PRIMITIVE_TYPES = new Class[]{Boolean.TYPE, Byte.TYPE, Short.TYPE, Character.TYPE, Integer.TYPE, Long.TYPE, Float.TYPE, Double.TYPE};

	private TreeMap<Integer,FieldType> mTypes;


	public TypeDeclarations(Object aEntity, HashMap<String, Field> mFields)
	{
		Log.v("create type declarations for " + aEntity.getClass());
		Log.inc();

		mTypes = new TreeMap<>();

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
			typeInfo.category = classify(field);

			mTypes.put(index, typeInfo);

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

			Log.v("type found: "+index+" "+typeInfo);

			index++;
		}

		Log.dec();
	}


	TreeMap<Integer, FieldType> getTypeDeclarations()
	{
		return mTypes;
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


	FieldType get(int aIndex)
	{
		return mTypes.get(aIndex);
	}


	int size()
	{
		return mTypes.size();
	}


	Iterable<Map.Entry<Integer, FieldType>> entrySet()
	{
		return mTypes.entrySet();
	}
}

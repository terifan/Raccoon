package org.terifan.raccoon.serialization.old;

import org.terifan.raccoon.serialization.old.FieldFormat;
import org.terifan.raccoon.serialization.old.FieldType;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.terifan.raccoon.DatabaseException;
import org.terifan.raccoon.Discriminator;
import org.terifan.raccoon.Key;
import org.terifan.raccoon.util.Log;


public class TypeDeclarations implements Externalizable
{
	private final static long serialVersionUID = 1L;
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

	private FieldType[] mTypes;


	public TypeDeclarations()
	{
	}


	public TypeDeclarations(Class aType, Field[] aFields)
	{
		Log.v("create type declarations for %s", aType);
		Log.inc();

		mTypes = new FieldType[aFields.length];

		int fieldIndex = 0;
		for (Field field : aFields)
		{
			FieldType fieldType = new FieldType();

			mTypes[fieldIndex++] = fieldType;

			fieldType.setName(field.getName());
			fieldType.setType(field.getType());
			boolean array = fieldType.getType().isArray();
			while (fieldType.getType().isArray())
			{
				fieldType.setType(fieldType.getType().getComponentType());
				fieldType.setDepth(fieldType.getDepth() + 1);
			}
			fieldType.setNullable(!fieldType.getType().isPrimitive());
			fieldType.setCategory(classify(field));

			if (array)
			{
				fieldType.setFormat(FieldFormat.ARRAY);
			}
			else if (List.class.isAssignableFrom(fieldType.getType()))
			{
				getGenericType(field, fieldType, 0);
				fieldType.setFormat(FieldFormat.LIST);
			}
			else if (Set.class.isAssignableFrom(fieldType.getType()))
			{
				getGenericType(field, fieldType, 0);
				fieldType.setFormat(FieldFormat.SET);
			}
			else if (Map.class.isAssignableFrom(fieldType.getType()))
			{
				getGenericType(field, fieldType, 0);
				getGenericType(field, fieldType, 1);
				fieldType.setFormat(FieldFormat.MAP);
			}
			else if (fieldType.getType().isPrimitive() || isValidType(fieldType.getType()))
			{
				fieldType.setFormat(FieldFormat.VALUE);
			}
			else
			{
				throw new IllegalArgumentException("Unsupported type: " + field);
			}

			Log.v("type found: %s", fieldType);
		}

		Log.dec();
	}


	public FieldType[] getTypes()
	{
		return mTypes;
	}


	@Override
	public void readExternal(ObjectInput aIn) throws IOException, ClassNotFoundException
	{
		mTypes = new FieldType[aIn.readInt()];
		for (int i = 0; i < mTypes.length; i++)
		{
			mTypes[i] = new FieldType();
			mTypes[i].readExternal(aIn);
		}
	}


	@Override
	public void writeExternal(ObjectOutput aOut) throws IOException
	{
		aOut.writeInt(mTypes.length);
		for (FieldType fieldType : mTypes)
		{
			fieldType.writeExternal(aOut);
		}
	}


	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		for (FieldType fieldType : mTypes)
		{
			if (sb.length() > 0)
			{
				sb.append("\n");
			}
			sb.append(fieldType);
		}
		return sb.toString();
	}


	private void getGenericType(Field aField, FieldType aFieldType, int aIndex)
	{
		if (!(aField.getGenericType() instanceof ParameterizedType))
		{
			throw new IllegalArgumentException("Generic type must be parameterized: " + aField);
		}

		if (aFieldType.getComponentType() == null)
		{
			aFieldType.setComponentType(new FieldType[2]);
		}

		FieldType componentType = new FieldType();
		aFieldType.getComponentType()[aIndex] = componentType;

		String typeName = ((ParameterizedType)aField.getGenericType()).getActualTypeArguments()[aIndex].getTypeName();

		while (typeName.endsWith("[]"))
		{
			typeName = typeName.substring(0, typeName.length() - 2);
			componentType.setDepth(componentType.getDepth() + 1);
			componentType.setFormat(FieldFormat.ARRAY);
		}

		Class primitiveType = PRIMITIVE_TYPES.get(typeName);
		componentType.setNullable(primitiveType == null);

		if (primitiveType != null)
		{
			componentType.setType(primitiveType);
			return;
		}

		try
		{
			Class<?> type = Class.forName(typeName);

			if (!isValidType(type))
			{
				throw new IllegalArgumentException("Unsupported type: " + type);
			}

			componentType.setType(type);
		}
		catch (ClassNotFoundException e)
		{
			throw new DatabaseException(e);
		}
	}


	private boolean isValidType(Class aType)
	{
		return Number.class.isAssignableFrom(aType)
			|| aType == String.class
			|| aType == Character.class
			|| aType == Boolean.class
			|| aType == Date.class
			|| Serializable.class.isAssignableFrom(aType);
	}


	private FieldCategory classify(Field aField)
	{
		if (aField.getAnnotation(Key.class) != null)
		{
			return FieldCategory.KEYS;
		}
		if (aField.getAnnotation(Discriminator.class) != null)
		{
			return FieldCategory.DISCRIMINATORS;
		}
		return FieldCategory.VALUES;
	}
}
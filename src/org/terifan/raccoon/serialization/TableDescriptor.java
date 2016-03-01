package org.terifan.raccoon.serialization;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import org.terifan.raccoon.Discriminator;
import org.terifan.raccoon.Key;
import org.terifan.raccoon.util.Log;


public class TableDescriptor implements Externalizable
{
	private String mName;
	private LinkedHashMap<Integer,FieldType> mFieldTypes;

	final static HashMap<Class,ContentType> VALUE_TYPES = new HashMap<>();
	final static HashMap<Class,ContentType> CLASS_TYPES = new HashMap<>();
	final static HashMap<ContentType,Class> TYPES = new HashMap<>();

	static
	{
		TYPES.put(ContentType.BOOLEAN, Boolean.TYPE);
		TYPES.put(ContentType.BYTE, Byte.TYPE);
		TYPES.put(ContentType.SHORT, Short.TYPE);
		TYPES.put(ContentType.CHAR, Character.TYPE);
		TYPES.put(ContentType.INT, Integer.TYPE);
		TYPES.put(ContentType.LONG, Long.TYPE);
		TYPES.put(ContentType.FLOAT, Float.TYPE);
		TYPES.put(ContentType.DOUBLE, Double.TYPE);
		TYPES.put(ContentType.STRING, String.class);
		TYPES.put(ContentType.DATE, Date.class);

		VALUE_TYPES.put(Boolean.TYPE, ContentType.BOOLEAN);
		VALUE_TYPES.put(Byte.TYPE, ContentType.BYTE);
		VALUE_TYPES.put(Short.TYPE, ContentType.SHORT);
		VALUE_TYPES.put(Character.TYPE, ContentType.CHAR);
		VALUE_TYPES.put(Integer.TYPE, ContentType.INT);
		VALUE_TYPES.put(Long.TYPE, ContentType.LONG);
		VALUE_TYPES.put(Float.TYPE, ContentType.FLOAT);
		VALUE_TYPES.put(Double.TYPE, ContentType.DOUBLE);

		CLASS_TYPES.put(Boolean.class, ContentType.BOOLEAN);
		CLASS_TYPES.put(Byte.class, ContentType.BYTE);
		CLASS_TYPES.put(Short.class, ContentType.SHORT);
		CLASS_TYPES.put(Character.class, ContentType.CHAR);
		CLASS_TYPES.put(Integer.class, ContentType.INT);
		CLASS_TYPES.put(Long.class, ContentType.LONG);
		CLASS_TYPES.put(Float.class, ContentType.FLOAT);
		CLASS_TYPES.put(Double.class, ContentType.DOUBLE);
	}


	public TableDescriptor()
	{
	}


	public TableDescriptor(Class aType)
	{
		Log.v("create type declarations for %s", aType);
		Log.inc();

		ArrayList<Field> fields = loadFields(aType);

		mName = aType.getName();
		mFieldTypes = new LinkedHashMap<>();
		int i = 0;

		for (Field field : fields)
		{
			FieldType fieldType = new FieldType();
			fieldType.setIndex(i);
			fieldType.setField(field);
			fieldType.setName(field.getName());
			fieldType.setDescription(field.getType().getName());

			categorizeContentType(field, fieldType);
			classifyContentType(field, fieldType);

			mFieldTypes.put(i++, fieldType);

			Log.v("type found: %s", fieldType);
		}

		Log.dec();
	}


	public void mapFields(Class aType)
	{
		for (Field field : loadFields(aType))
		{
			for (FieldType fieldType : mFieldTypes.values())
			{
				if (fieldType.getName().equals(field.getName()))
				{
					fieldType.setField(field);
				}
			}
		}
	}


	private ArrayList<Field> loadFields(Class aType)
	{
		ArrayList<Field> fields = new ArrayList<>();

		for (Field field : aType.getDeclaredFields())
		{
			if ((field.getModifiers() & (Modifier.TRANSIENT | Modifier.STATIC | Modifier.FINAL)) != 0)
			{
				continue;
			}

			field.setAccessible(true);
			fields.add(field);
		}

		return fields;
	}


	public HashMap<Integer,FieldType> getTypes()
	{
		return mFieldTypes;
	}


	public String getName()
	{
		return mName;
	}


	@Override
	public void readExternal(ObjectInput aIn) throws IOException, ClassNotFoundException
	{
		mFieldTypes = new LinkedHashMap<>();

		mName = aIn.readUTF();
		short len = aIn.readShort();

		for (int i = 0; i < len; i++)
		{
			FieldType tmp = new FieldType();
			tmp.readExternal(aIn);
			mFieldTypes.put(tmp.getIndex(), tmp);
		}
	}


	@Override
	public void writeExternal(ObjectOutput aOut) throws IOException
	{
		aOut.writeUTF(mName);
		aOut.writeShort(mFieldTypes.size());

		for (FieldType fieldType : mFieldTypes.values())
		{
			fieldType.writeExternal(aOut);
		}
	}


	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		for (FieldType fieldType : mFieldTypes.values())
		{
			if (sb.length() > 0)
			{
				sb.append("\n");
			}
			sb.append(fieldType);
		}
		return sb.toString();
	}


	private void categorizeContentType(Field aField, FieldType aFieldType)
	{
		if (aField.getAnnotation(Discriminator.class) != null)
		{
			aFieldType.setCategory(FieldCategory.DISCRIMINATOR);
		}
		else if (aField.getAnnotation(Key.class) != null)
		{
			aFieldType.setCategory(FieldCategory.KEY);
		}
		else
		{
			aFieldType.setCategory(FieldCategory.VALUE);
		}
	}


	private void classifyContentType(Field aField, FieldType aFieldType)
	{
		Class<?> type = aField.getType();

		while (type.isArray())
		{
			aFieldType.setArray(true);
			aFieldType.setDepth(aFieldType.getDepth() + 1);
			type = type.getComponentType();
		}

		if (type.isPrimitive())
		{
			ContentType primitiveType = VALUE_TYPES.get(type);
			if (primitiveType != null)
			{
				aFieldType.setContentType(primitiveType);
				return;
			}
		}

		aFieldType.setNullable(true);

		ContentType contentType = CLASS_TYPES.get(type);
		if (contentType != null)
		{
			aFieldType.setContentType(contentType);
		}
		else if (type == String.class)
		{
			aFieldType.setContentType(ContentType.STRING);
		}
		else if (Date.class.isAssignableFrom(type))
		{
			aFieldType.setContentType(ContentType.DATE);
		}
		else
		{
			aFieldType.setContentType(ContentType.OBJECT);
		}
	}
}
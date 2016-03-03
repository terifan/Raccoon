package org.terifan.raccoon.serialization;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import org.terifan.raccoon.Discriminator;
import org.terifan.raccoon.Key;
import org.terifan.raccoon.util.Log;


public class EntityDescriptor implements Serializable// implements Externalizable
{
	private static final long serialVersionUID = 1L;

	private String mName;
	private FieldType[] mFieldTypes;
	private FieldType[] mKeyFields;
	private FieldType[] mDiscriminatorFields;
	private FieldType[] mValueFields;

	final static HashMap<Class,ContentType> VALUE_TYPES = new HashMap<>();
	final static HashMap<Class,ContentType> CLASS_TYPES = new HashMap<>();
	final static HashMap<ContentType,Class> TYPE_VALUES = new HashMap<>();
	final static HashMap<ContentType,Class> TYPE_CLASSES = new HashMap<>();

	static
	{
		TYPE_VALUES.put(ContentType.BOOLEAN, Boolean.TYPE);
		TYPE_VALUES.put(ContentType.BYTE, Byte.TYPE);
		TYPE_VALUES.put(ContentType.SHORT, Short.TYPE);
		TYPE_VALUES.put(ContentType.CHAR, Character.TYPE);
		TYPE_VALUES.put(ContentType.INT, Integer.TYPE);
		TYPE_VALUES.put(ContentType.LONG, Long.TYPE);
		TYPE_VALUES.put(ContentType.FLOAT, Float.TYPE);
		TYPE_VALUES.put(ContentType.DOUBLE, Double.TYPE);
		TYPE_VALUES.put(ContentType.STRING, String.class);
		TYPE_VALUES.put(ContentType.DATE, Date.class);

		TYPE_CLASSES.put(ContentType.BOOLEAN, Boolean.class);
		TYPE_CLASSES.put(ContentType.BYTE, Byte.class);
		TYPE_CLASSES.put(ContentType.SHORT, Short.class);
		TYPE_CLASSES.put(ContentType.CHAR, Character.class);
		TYPE_CLASSES.put(ContentType.INT, Integer.class);
		TYPE_CLASSES.put(ContentType.LONG, Long.class);
		TYPE_CLASSES.put(ContentType.FLOAT, Float.class);
		TYPE_CLASSES.put(ContentType.DOUBLE, Double.class);
		TYPE_CLASSES.put(ContentType.STRING, String.class);
		TYPE_CLASSES.put(ContentType.DATE, Date.class);

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


	public EntityDescriptor()
	{
	}


	public EntityDescriptor(Class aType)
	{
		Log.v("create type declarations for %s", aType);
		Log.inc();

		ArrayList<Field> fields = loadFields(aType);

		mName = aType.getName();
		ArrayList<FieldType> tmp = new ArrayList<>();
		ArrayList<FieldType> tmpK = new ArrayList<>();
		ArrayList<FieldType> tmpD = new ArrayList<>();
		ArrayList<FieldType> tmpV = new ArrayList<>();
		int i = 0;

		for (Field field : fields)
		{
			FieldType fieldType = new FieldType();
			fieldType.setIndex(i++);
			fieldType.setField(field);
			fieldType.setName(field.getName());
			fieldType.setDescription(field.getType().getName());

			categorizeContentType(field, fieldType);
			classifyContentType(field, fieldType);

			tmp.add(fieldType);

			if (fieldType.getCategory() == FieldCategory.KEY) tmpK.add(fieldType);
			if (fieldType.getCategory() == FieldCategory.DISCRIMINATOR) tmpD.add(fieldType);
			if (fieldType.getCategory() != FieldCategory.KEY) tmpV.add(fieldType);

			Log.v("type found: %s", fieldType);
		}

		mFieldTypes = tmp.toArray(new FieldType[tmp.size()]);

		mKeyFields = tmpK.toArray(new FieldType[tmpK.size()]);
		mDiscriminatorFields = tmpD.toArray(new FieldType[tmpD.size()]);
		mValueFields = tmpV.toArray(new FieldType[tmpV.size()]);

		Log.dec();
	}


	public void mapFields(Class aType)
	{
		for (Field field : loadFields(aType))
		{
			for (FieldType fieldType : mFieldTypes)
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


	public FieldType[] getTypes()
	{
		return mFieldTypes;
	}


	public String getName()
	{
		return mName;
	}


//	@Override
//	public void readExternal(ObjectInput aIn) throws IOException, ClassNotFoundException
//	{
//		mName = aIn.readUTF();
//		short len = aIn.readShort();
//
//		mFieldTypes = new FieldType[len];
//
//		for (int i = 0; i < len; i++)
//		{
//			FieldType tmp = new FieldType();
//			tmp.readExternal(aIn);
//			mFieldTypes[tmp.getIndex()] = tmp;
//		}
//	}
//
//
//	@Override
//	public void writeExternal(ObjectOutput aOut) throws IOException
//	{
//		aOut.writeUTF(mName);
//		aOut.writeShort(mFieldTypes.length);
//
//		for (FieldType fieldType : mFieldTypes)
//		{
//			fieldType.writeExternal(aOut);
//		}
//	}


	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		for (FieldType fieldType : mFieldTypes)
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


	FieldType[] getKeyFields()
	{
		return mKeyFields;
	}


	FieldType[] getDiscriminatorFields()
	{
		return mDiscriminatorFields;
	}


	FieldType[] getValueFields()
	{
		return mValueFields;
	}
}
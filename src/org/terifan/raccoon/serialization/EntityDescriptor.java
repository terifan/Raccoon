package org.terifan.raccoon.serialization;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Date;
import org.terifan.raccoon.Discriminator;
import org.terifan.raccoon.Key;
import org.terifan.raccoon.util.Log;
import static org.terifan.raccoon.serialization.TypeMappings.*;


public class EntityDescriptor implements Externalizable
{
	private static final long serialVersionUID = 1L;

	private String mName;
	private FieldType[] mFieldTypes;

	private transient FieldType[] mKeyFields;
	private transient FieldType[] mDiscriminatorFields;
	private transient FieldType[] mValueFields;


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

		for (Field field : fields)
		{
			FieldType fieldType = new FieldType();
			fieldType.setIndex(tmp.size());
			fieldType.setField(field);
			fieldType.setName(field.getName());
			fieldType.setTypeName(field.getType().getName());

			categorize(field, fieldType);
			classify(field, fieldType);

			tmp.add(fieldType);

			Log.v("type found: %s", fieldType);
		}

		mFieldTypes = tmp.toArray(new FieldType[tmp.size()]);

		updateLookupTables();

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


	public FieldType[] getTypes()
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
		mName = aIn.readUTF();
		mFieldTypes = (FieldType[])aIn.readObject();

		updateLookupTables();
	}


	@Override
	public void writeExternal(ObjectOutput aOut) throws IOException
	{
		aOut.writeUTF(mName);
		aOut.writeObject(mFieldTypes);
	}


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


	private ArrayList<Field> loadFields(Class aType)
	{
		ArrayList<Field> fields = new ArrayList<>();

		for (Field field : aType.getDeclaredFields())
		{
			if ((field.getModifiers() & (Modifier.TRANSIENT | Modifier.STATIC | Modifier.FINAL)) == 0)
			{
				field.setAccessible(true);
				fields.add(field);
			}
		}

		return fields;
	}


	private void categorize(Field aField, FieldType aFieldType)
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


	private void classify(Field aField, FieldType aFieldType)
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


	private void updateLookupTables()
	{
		ArrayList<FieldType> tmpK = new ArrayList<>();
		ArrayList<FieldType> tmpD = new ArrayList<>();
		ArrayList<FieldType> tmpV = new ArrayList<>();

		for (FieldType fieldType : mFieldTypes)
		{
			if (fieldType.getCategory() == FieldCategory.KEY) tmpK.add(fieldType);
			if (fieldType.getCategory() == FieldCategory.DISCRIMINATOR) tmpD.add(fieldType);
			if (fieldType.getCategory() != FieldCategory.KEY) tmpV.add(fieldType);
		}

		mKeyFields = tmpK.toArray(new FieldType[tmpK.size()]);
		mDiscriminatorFields = tmpD.toArray(new FieldType[tmpD.size()]);
		mValueFields = tmpV.toArray(new FieldType[tmpV.size()]);
	}
}
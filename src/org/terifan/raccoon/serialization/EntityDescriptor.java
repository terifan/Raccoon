package org.terifan.raccoon.serialization;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Objects;
import org.terifan.raccoon.Discriminator;
import org.terifan.raccoon.Key;
import org.terifan.raccoon.util.Log;
import static org.terifan.raccoon.serialization.TypeMappings.*;


public class EntityDescriptor implements Externalizable
{
	private static final long serialVersionUID = 1L;

	private String mName;
	private FieldDescriptor[] mFieldDescriptors;

	private transient Class mType;
	private transient FieldDescriptor[] mKeyFields;
	private transient FieldDescriptor[] mDiscriminatorFields;
	private transient FieldDescriptor[] mValueFields;


	public EntityDescriptor()
	{
	}


	EntityDescriptor(Class aType)
	{
		Log.v("create type declarations for %s", aType);
		Log.inc();

		ArrayList<Field> fields = loadFields(aType);

		mType = aType;
		mName = aType.getName();

		ArrayList<FieldDescriptor> tmp = new ArrayList<>();

		for (Field field : fields)
		{
			FieldDescriptor fieldType = new FieldDescriptor();
			fieldType.setField(field);
			fieldType.setName(field.getName());
			fieldType.setTypeName(field.getType().getName());
			fieldType.setIndex(tmp.size());

			categorize(field, fieldType);
			classify(field, fieldType);

			tmp.add(fieldType);

			Log.v("type found: %s", fieldType);
		}

		initializeFieldTypeLists(tmp.toArray(new FieldDescriptor[tmp.size()]));

		Log.dec();
	}


	public Class getType()
	{
		return mType;
	}


	public void setType(Class aType)
	{
		this.mType = aType;
	}


	public FieldDescriptor[] getTypes()
	{
		return mFieldDescriptors;
	}


	public String getName()
	{
		return mName;
	}


	@Override
	public void readExternal(ObjectInput aIn) throws IOException, ClassNotFoundException
	{
		mName = aIn.readUTF();

		initializeFieldTypeLists((FieldDescriptor[])aIn.readObject());
	}


	@Override
	public void writeExternal(ObjectOutput aOut) throws IOException
	{
		aOut.writeUTF(mName);
		aOut.writeObject(mFieldDescriptors);
	}


	@Override
	public boolean equals(Object aObj)
	{
		if (aObj instanceof EntityDescriptor)
		{
			EntityDescriptor other = (EntityDescriptor)aObj;

			return mName.equals(other.mName) && Arrays.equals(mFieldDescriptors, other.mFieldDescriptors);
		}

		return false;
	}


	@Override
	public int hashCode()
	{
		return Objects.hashCode(mName) ^ Arrays.deepHashCode(mFieldDescriptors);
	}


	/**
	 * Return an entity as a Java class declaration.
	 */
	public String getJavaDeclaration()
	{
		StringBuilder sb = new StringBuilder();
		sb.append("package " + mName.substring(0, mName.lastIndexOf(".")) + ";\n\n");
		sb.append("class " + mName.substring(mName.lastIndexOf(".") + 1) + "\n{\n");

		for (FieldDescriptor fieldType : mFieldDescriptors)
		{
			String annotation;

			switch (fieldType.getCategory())
			{
				case KEY:
					annotation = "@Key ";
					break;
				case DISCRIMINATOR:
					annotation = "@Discriminator ";
					break;
				default:
					annotation = "";
					break;
			}

			sb.append("\t" + annotation + fieldType + ";\n");
		}

		sb.append("}");

		return sb.toString();
	}


	FieldDescriptor[] getKeyFields()
	{
		return mKeyFields;
	}


	FieldDescriptor[] getDiscriminatorFields()
	{
		return mDiscriminatorFields;
	}


	FieldDescriptor[] getValueFields()
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


	private void categorize(Field aField, FieldDescriptor aFieldType)
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


	private void classify(Field aField, FieldDescriptor aFieldType)
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
			FieldType primitiveType = VALUE_TYPES.get(type);
			if (primitiveType != null)
			{
				aFieldType.setContentType(primitiveType);
				return;
			}
		}

		aFieldType.setNullable(true);

		FieldType contentType = CLASS_TYPES.get(type);
		if (contentType != null)
		{
			aFieldType.setContentType(contentType);
		}
		else if (type == String.class)
		{
			aFieldType.setContentType(FieldType.STRING);
		}
		else if (Date.class.isAssignableFrom(type))
		{
			aFieldType.setContentType(FieldType.DATE);
		}
		else
		{
			aFieldType.setContentType(FieldType.OBJECT);
		}
	}


	private void initializeFieldTypeLists(FieldDescriptor[] aFieldTypes)
	{
		ArrayList<FieldDescriptor> keys = new ArrayList<>();
		ArrayList<FieldDescriptor> disc = new ArrayList<>();
		ArrayList<FieldDescriptor> values = new ArrayList<>();

		for (FieldDescriptor fieldType : aFieldTypes)
		{
			if (fieldType.getCategory() == FieldCategory.KEY)
			{
				keys.add(fieldType);
			}
			if (fieldType.getCategory() == FieldCategory.DISCRIMINATOR)
			{
				disc.add(fieldType);
			}
			if (fieldType.getCategory() != FieldCategory.KEY)
			{
				values.add(fieldType);
			}
		}

		if (keys.isEmpty())
		{
			throw new IllegalArgumentException("Entity has no keys: " + mName + Arrays.toString(mFieldDescriptors));
		}

		mFieldDescriptors = aFieldTypes;
		mKeyFields = keys.toArray(new FieldDescriptor[keys.size()]);
		mDiscriminatorFields = disc.toArray(new FieldDescriptor[disc.size()]);
		mValueFields = values.toArray(new FieldDescriptor[values.size()]);

		if (mType != null)
		{
			loadFields();
		}
	}


	private Field getField(FieldDescriptor aFieldType)
	{
		assert mType != null : "Internal Error: Type not bound!";

		Field field = aFieldType.getField();

		if (field == null)
		{
			for (Field f : mType.getDeclaredFields())
			{
				if (aFieldType.getName().equals(f.getName()) && aFieldType.getTypeName().equals(f.getType().getName()))
				{
					field = f;
					aFieldType.setField(field);
					field.setAccessible(true);
					break;
				}
			}
		}

		return field;
	}


	private void loadFields()
	{
		for (FieldDescriptor field : mFieldDescriptors)
		{
			getField(field);
		}
		for (FieldDescriptor field : mKeyFields)
		{
			getField(field);
		}
		for (FieldDescriptor field : mDiscriminatorFields)
		{
			getField(field);
		}
		for (FieldDescriptor field : mValueFields)
		{
			getField(field);
		}
	}


	@Override
	public String toString()
	{
		return "EntityDescriptor{" + "mName=" + mName + ", mFieldTypes=" + Arrays.toString(mFieldDescriptors) + ", mKeyFields=" + Arrays.toString(mKeyFields) + ", mDiscriminatorFields=" + Arrays.toString(mDiscriminatorFields) + ", mValueFields=" + Arrays.toString(mValueFields) + '}';
	}
}

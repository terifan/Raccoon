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
import java.util.HashMap;
import java.util.Objects;
import org.terifan.raccoon.Discriminator;
import org.terifan.raccoon.Key;
import org.terifan.raccoon.util.Log;
import static org.terifan.raccoon.serialization.TypeMappings.*;


public class EntityDescriptor implements Externalizable
{
	private static final long serialVersionUID = 1L;

	private final static HashMap<Class,EntityDescriptor> mEntityDescriptors = new HashMap<>();

	private String mName;
	private FieldType[] mFieldTypes;

	private transient Class mType;
	private transient FieldType[] mKeyFields;
	private transient FieldType[] mDiscriminatorFields;
	private transient FieldType[] mValueFields;


	public EntityDescriptor()
	{
	}


	private EntityDescriptor(Class aType)
	{
		Log.v("create type declarations for %s", aType);
		Log.inc();

		ArrayList<Field> fields = loadFields(aType);

		mType = aType;
		mName = aType.getName();
		ArrayList<FieldType> tmp = new ArrayList<>();

		for (Field field : fields)
		{
			FieldType fieldType = new FieldType();
			fieldType.setField(field);
			fieldType.setName(field.getName());
			fieldType.setTypeName(field.getType().getName());
			fieldType.setIndex(tmp.size());

			categorize(field, fieldType);
			classify(field, fieldType);

			tmp.add(fieldType);

			Log.v("type found: %s", fieldType);
		}

		initializeFieldTypeLists(tmp.toArray(new FieldType[tmp.size()]));

		Log.dec();
	}


	public static synchronized EntityDescriptor getInstance(Class aType)
	{
		EntityDescriptor instance = mEntityDescriptors.get(aType);

		if (instance == null)
		{
			instance = new EntityDescriptor(aType);
			mEntityDescriptors.put(aType, instance);
		}

		return instance;
	}


	public Class getType()
	{
		return mType;
	}


	public void setType(Class aType)
	{
		this.mType = aType;
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

		initializeFieldTypeLists((FieldType[])aIn.readObject());
	}


	@Override
	public void writeExternal(ObjectOutput aOut) throws IOException
	{
		aOut.writeUTF(mName);
		aOut.writeObject(mFieldTypes);
	}


	@Override
	public boolean equals(Object aObj)
	{
		if (aObj instanceof EntityDescriptor)
		{
			EntityDescriptor other = (EntityDescriptor)aObj;

			return mName.equals(other.mName)
				&& Arrays.equals(mFieldTypes, other.mFieldTypes);
		}

		return false;
	}


	@Override
	public int hashCode()
	{
		return Objects.hashCode(mName) ^ Arrays.deepHashCode(mFieldTypes);
	}


	/**
	 * Return an entity as a Java class declaration.
	 */
	public String getJavaDeclaration()
	{
		StringBuilder sb = new StringBuilder();
		sb.append("package " + mName.substring(0, mName.lastIndexOf(".")) + ";\n\n");
		sb.append("class " + mName.substring(mName.lastIndexOf(".")+1) + "\n{\n");
		for (FieldType fieldType : mFieldTypes)
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


	private void initializeFieldTypeLists(FieldType[] aFieldTypes)
	{
		ArrayList<FieldType> tmpK = new ArrayList<>();
		ArrayList<FieldType> tmpD = new ArrayList<>();
		ArrayList<FieldType> tmpV = new ArrayList<>();

		for (FieldType fieldType : aFieldTypes)
		{
			if (fieldType.getCategory() == FieldCategory.KEY) tmpK.add(fieldType);
			if (fieldType.getCategory() == FieldCategory.DISCRIMINATOR) tmpD.add(fieldType);
			if (fieldType.getCategory() != FieldCategory.KEY) tmpV.add(fieldType);
		}

		mFieldTypes = aFieldTypes;
		mKeyFields = tmpK.toArray(new FieldType[tmpK.size()]);
		mDiscriminatorFields = tmpD.toArray(new FieldType[tmpD.size()]);
		mValueFields = tmpV.toArray(new FieldType[tmpV.size()]);

		if (mKeyFields.length == 0)
		{
			throw new IllegalArgumentException("Entity has no keys: " + mName + Arrays.toString(mFieldTypes));
		}
	}


	@Override
	public String toString()
	{
		return "EntityDescriptor{" + "mName=" + mName + ", mFieldTypes=" + Arrays.toString(mFieldTypes) + ", mKeyFields=" + Arrays.toString(mKeyFields) + ", mDiscriminatorFields=" + Arrays.toString(mDiscriminatorFields) + ", mValueFields=" + Arrays.toString(mValueFields) + '}';
	}


	Field getField(FieldType aFieldType)
	{
		Field field = aFieldType.getField();

		if (field == null)
		{
			if (mType == null)
			{
				throw new IllegalStateException("Internal Error: Type not bound!");
			}

			for (Field f : mType.getDeclaredFields())
			{
				// (field.getModifiers() & (Modifier.TRANSIENT | Modifier.STATIC | Modifier.FINAL)) == 0 &&
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
}
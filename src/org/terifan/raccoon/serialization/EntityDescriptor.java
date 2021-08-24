package org.terifan.raccoon.serialization;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Objects;
import org.terifan.raccoon.annotations.Column;
import org.terifan.raccoon.annotations.Discriminator;
import org.terifan.raccoon.annotations.Id;
import static org.terifan.raccoon.serialization.TypeMappings.VALUE_TYPES;
import static org.terifan.raccoon.serialization.TypeMappings.CLASS_TYPES;
import org.terifan.raccoon.util.Log;


public class EntityDescriptor implements Externalizable
{
	private static final long serialVersionUID = 1L;

	private String mEntityName;
	private String mTypeName;
	private String mPackageName;
	private FieldDescriptor[] mFields;


	/**
	 * Use only for serialization.
	 */
	public EntityDescriptor()
	{
	}


	public EntityDescriptor(String aEntityName, Class aType, FieldTypeCategorizer aCategorizer)
	{
		Log.d("create type declarations for %s", aType);
		Log.inc();

		mEntityName = aEntityName;
		mTypeName = aType.getName();
		mPackageName = aType.getPackage().getName();
		mFields = extractFields(aType, aCategorizer);

		Log.dec();
	}


	public void bind(Class aType)
	{
		for (Field field : ObjectReflection.getDeclaredFields(aType))
		{
			for (FieldDescriptor fieldDescriptor : mFields)
			{
				if (fieldDescriptor.getFieldName().equals(field.getName()) && fieldDescriptor.getTypeName().equals(field.getType().getName()))
				{
					fieldDescriptor.setField(field);
				}
			}
		}
	}


	public String getEntityName()
	{
		return mEntityName;
	}


	public String getTypeName()
	{
		return mTypeName;
	}


	public String getPackageName()
	{
		return mPackageName;
	}


	public FieldDescriptor[] getFields()
	{
		return mFields.clone();
	}


	/**
	 * Return fields of one or more categories
	 *
	 * @param aCategory
	 *   bit field for category
	 */
	public ArrayList<FieldDescriptor> getFields(int aCategory)
	{
		ArrayList<FieldDescriptor> fd = new ArrayList<>();
		for (FieldDescriptor field : mFields)
		{
			if ((field.getCategory() & aCategory) != 0)
			{
				fd.add(field);
			}
		}
		return fd;
	}


	@Override
	public void readExternal(ObjectInput aIn) throws IOException, ClassNotFoundException
	{
		mEntityName = aIn.readUTF();
		mTypeName = aIn.readUTF();
		mPackageName = aIn.readUTF();
		mFields = (FieldDescriptor[])aIn.readObject();
	}


	@Override
	public void writeExternal(ObjectOutput aOut) throws IOException
	{
		aOut.writeUTF(mEntityName);
		aOut.writeUTF(mTypeName);
		aOut.writeUTF(mPackageName);
		aOut.writeObject(mFields);
	}


	@Override
	public boolean equals(Object aObj)
	{
		if (aObj instanceof EntityDescriptor)
		{
			EntityDescriptor other = (EntityDescriptor)aObj;

			return mEntityName.equals(other.mEntityName) && Arrays.equals(mFields, other.mFields);
		}

		return false;
	}


	@Override
	public int hashCode()
	{
		return Objects.hashCode(mEntityName) ^ Arrays.deepHashCode(mFields);
	}


	@Override
	public String toString()
	{
		return "EntityDescriptor{" + "mEntityName=" + mEntityName + ", mPackageName=" + mPackageName + ", mTypeName=" + mTypeName + ", mFields=" + Arrays.toString(mFields) + '}';
	}


	private static FieldDescriptor[] extractFields(Class aType, FieldTypeCategorizer aCategorizer)
	{
		ArrayList<Field> fields = new ArrayList<>();
		for (Field field : ObjectReflection.getDeclaredFields(aType))
		{
			fields.add(field);
		}

		ArrayList<FieldDescriptor> fieldDescriptors = new ArrayList<>();
		int index = 0;

		for (Field field : fields)
		{
			String columnName = "";

			Column column = field.getAnnotation(Column.class);
			if (column != null && !column.name().isEmpty())
			{
				columnName = column.name();
			}

			Id id = field.getAnnotation(Id.class);
			if (id != null && !id.name().isEmpty())
			{
				columnName = id.name();
			}

			Discriminator disc = field.getAnnotation(Discriminator.class);
			if (disc != null && !disc.name().isEmpty())
			{
				columnName = disc.name();
			}

			Class<?> tmp = field.getType();
			while (tmp.isArray())
			{
				tmp = tmp.getComponentType();
			}
			String typeName = tmp.getName();

			FieldDescriptor fieldDescriptor = new FieldDescriptor();
			fieldDescriptor.setField(field);
			fieldDescriptor.setFieldName(field.getName());
			fieldDescriptor.setColumnName(columnName);
			fieldDescriptor.setTypeName(typeName);
			fieldDescriptor.setIndex(index++);
			fieldDescriptor.setCategory(aCategorizer.categorize(field));

			classify(field, fieldDescriptor);

			fieldDescriptors.add(fieldDescriptor);

			Log.d("type found: %s", fieldDescriptor);
		}

		return fieldDescriptors.toArray(new FieldDescriptor[fieldDescriptors.size()]);
	}


	private static void classify(Field aField, FieldDescriptor aFieldDescriptor)
	{
		Class<?> type = aField.getType();

		while (type.isArray())
		{
			aFieldDescriptor.setArray(true);
			aFieldDescriptor.setNullable(true);
			aFieldDescriptor.setDepth(aFieldDescriptor.getDepth() + 1);
			type = type.getComponentType();
		}

		if (type.isPrimitive())
		{
			ValueType primitiveType = VALUE_TYPES.get(type);
			if (primitiveType != null)
			{
				aFieldDescriptor.setPrimitive(true);
				aFieldDescriptor.setValueType(primitiveType);
				return;
			}
		}

		aFieldDescriptor.setNullable(true);

		ValueType valueType = CLASS_TYPES.get(type);
		if (valueType != null)
		{
			aFieldDescriptor.setValueType(valueType);
		}
		else if (type == String.class)
		{
			aFieldDescriptor.setValueType(ValueType.STRING);
		}
		else if (Date.class.isAssignableFrom(type))
		{
			aFieldDescriptor.setValueType(ValueType.DATE);
		}
		else
		{
			aFieldDescriptor.setValueType(ValueType.OBJECT);
		}
	}
}

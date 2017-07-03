package org.terifan.raccoon.serialization;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Objects;
import static org.terifan.raccoon.serialization.TypeMappings.VALUE_TYPES;
import static org.terifan.raccoon.serialization.TypeMappings.CLASS_TYPES;
import org.terifan.raccoon.util.Log;


public class EntityDescriptor implements Externalizable
{
	private static final long serialVersionUID = 1L;

	private String mName;
	private FieldDescriptor[] mFields;


	/**
	 * Use only for serialization.
	 */
	public EntityDescriptor()
	{
	}


	public EntityDescriptor(Class aType, FieldTypeCategorizer aCategorizer)
	{
		Log.d("create type declarations for %s", aType);
		Log.inc();

		mName = aType.getName();
		mFields = extractFields(aType, aCategorizer);

		Log.dec();
	}


	public void bind(Class aType)
	{
		for (Field field : ObjectReflection.getDeclaredFields(aType))
		{
			for (FieldDescriptor fieldDescriptor : mFields)
			{
				if (fieldDescriptor.getName().equals(field.getName()) && fieldDescriptor.getTypeName().equals(field.getType().getName()))
				{
					fieldDescriptor.setField(field);
				}
			}
		}
	}


	public String getName()
	{
		return mName;
	}


	// TODO: use iterator
	@Deprecated
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
		mName = aIn.readUTF();
		mFields = (FieldDescriptor[])aIn.readObject();
	}


	@Override
	public void writeExternal(ObjectOutput aOut) throws IOException
	{
		aOut.writeUTF(mName);
		aOut.writeObject(mFields);
	}


	@Override
	public boolean equals(Object aObj)
	{
		if (aObj instanceof EntityDescriptor)
		{
			EntityDescriptor other = (EntityDescriptor)aObj;

			return mName.equals(other.mName) && Arrays.equals(mFields, other.mFields);
		}

		return false;
	}


	@Override
	public int hashCode()
	{
		return Objects.hashCode(mName) ^ Arrays.deepHashCode(mFields);
	}


	@Override
	public String toString()
	{
		return "EntityDescriptor{" + "mName=" + mName + ", mFields=" + Arrays.toString(mFields) + '}';
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
			FieldDescriptor fieldDescriptor = new FieldDescriptor();
			fieldDescriptor.setField(field);
			fieldDescriptor.setName(field.getName());
			fieldDescriptor.setTypeName(field.getType().getName());
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

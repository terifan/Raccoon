package org.terifan.raccoon.serialization;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import org.terifan.raccoon.Discriminator;
import org.terifan.raccoon.Key;
import org.terifan.raccoon.util.Log;
import static org.terifan.raccoon.serialization.TypeMappings.VALUE_TYPES;
import static org.terifan.raccoon.serialization.TypeMappings.*;


public class EntityDescriptorFactory
{
	private final static HashMap<Class, EntityDescriptor> INSTANCES = new HashMap<>();


	private EntityDescriptorFactory()
	{
	}


	public static synchronized EntityDescriptor getInstance(Class aType)
	{
		EntityDescriptor instance = INSTANCES.get(aType);

		if (instance == null)
		{
			instance = createEntityDescriptor(aType);
			INSTANCES.put(aType, instance);
		}

		return instance;
	}


	private static EntityDescriptor createEntityDescriptor(Class aType)
	{
		ArrayList<Field> fields = new ArrayList<>();

		for (Field field : ObjectReflection.getDeclaredFields(aType))
		{
			fields.add(field);
		}

		ArrayList<FieldDescriptor> keys = new ArrayList<>();
		ArrayList<FieldDescriptor> discriminators = new ArrayList<>();
		ArrayList<FieldDescriptor> values = new ArrayList<>();
		int index = 0;

		for (Field field : fields)
		{
			FieldDescriptor fieldDescriptor = new FieldDescriptor();
			fieldDescriptor.setField(field);
			fieldDescriptor.setName(field.getName());
			fieldDescriptor.setTypeName(field.getType().getName());
			fieldDescriptor.setIndex(index++);
			fieldDescriptor.setCategory(getCategory(field));

			classify(field, fieldDescriptor);

			switch (fieldDescriptor.getCategory())
			{
				case DISCRIMINATOR:
					discriminators.add(fieldDescriptor);
					break;
				case KEY:
					keys.add(fieldDescriptor);
					break;
				default:
					values.add(fieldDescriptor);
					break;
			}

			Log.v("type found: %s", fieldDescriptor);
		}

		if (keys.isEmpty())
		{
			throw new IllegalArgumentException("Entity has no keys: " + aType);
		}

		return new EntityDescriptor(aType, keys.toArray(new FieldDescriptor[keys.size()]), discriminators.toArray(new FieldDescriptor[discriminators.size()]), values.toArray(new FieldDescriptor[values.size()]));
	}


	private static FieldCategory getCategory(Field aField)
	{
		if (aField.getAnnotation(Discriminator.class) != null)
		{
			return FieldCategory.DISCRIMINATOR;
		}
		else if (aField.getAnnotation(Key.class) != null)
		{
			return FieldCategory.KEY;
		}
		return FieldCategory.VALUE;
	}


	private static void classify(Field aField, FieldDescriptor aFieldDescriptor)
	{
		Class<?> type = aField.getType();

		while (type.isArray())
		{
			aFieldDescriptor.setArray(true);
			aFieldDescriptor.setDepth(aFieldDescriptor.getDepth() + 1);
			type = type.getComponentType();
		}

		if (type.isPrimitive())
		{
			ValueType primitiveType = VALUE_TYPES.get(type);
			if (primitiveType != null)
			{
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

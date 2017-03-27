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


	public static synchronized EntityDescriptor getInstance(Class aType, FieldTypeCategorizer aCategorizer)
	{
		EntityDescriptor instance = INSTANCES.get(aType);

		if (instance == null)
		{
			instance = createEntityDescriptor(aType, aCategorizer);
			INSTANCES.put(aType, instance);
		}

		return instance;
	}


	private static EntityDescriptor createEntityDescriptor(Class aType, FieldTypeCategorizer aCategorizer)
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

			Log.v("type found: %s", fieldDescriptor);
		}

		return new EntityDescriptor(aType, fieldDescriptors.toArray(new FieldDescriptor[fieldDescriptors.size()]));
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

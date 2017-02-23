package org.terifan.raccoon.serialization;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import org.terifan.raccoon.Discriminator;
import org.terifan.raccoon.Key;
import static org.terifan.raccoon.serialization.TypeMappings.VALUE_TYPES;
import org.terifan.raccoon.util.Log;
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

			classify(field, fieldDescriptor);

			if (field.getAnnotation(Discriminator.class) != null)
			{
				fieldDescriptor.setCategory(FieldCategory.DISCRIMINATOR);
				discriminators.add(fieldDescriptor);
			}
			else if (field.getAnnotation(Key.class) != null)
			{
				fieldDescriptor.setCategory(FieldCategory.KEY);
				keys.add(fieldDescriptor);
			}
			else
			{
				fieldDescriptor.setCategory(FieldCategory.VALUE);
				values.add(fieldDescriptor);
			}

			Log.v("type found: %s", fieldDescriptor);
		}

		if (keys.isEmpty())
		{
			throw new IllegalArgumentException("Entity has no keys: " + aType);
		}

		return new EntityDescriptor(aType, keys.toArray(new FieldDescriptor[keys.size()]), discriminators.toArray(new FieldDescriptor[discriminators.size()]), values.toArray(new FieldDescriptor[values.size()]));
	}


	private static void classify(Field aField, FieldDescriptor aFieldType)
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
}

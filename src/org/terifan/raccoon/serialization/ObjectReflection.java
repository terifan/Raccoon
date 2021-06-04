package org.terifan.raccoon.serialization;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import org.terifan.raccoon.annotations.Column;
import org.terifan.raccoon.annotations.Discriminator;
import org.terifan.raccoon.annotations.Entity;
import org.terifan.raccoon.annotations.Id;


class ObjectReflection
{
	public static ArrayList<Field> getDeclaredFields(Class<?> aType)
	{
		return getDeclaredFields(aType, new ArrayList<>());
	}


	private static ArrayList<Field> getDeclaredFields(Class<?> aType, ArrayList<Field> aOutput)
	{
		if (aType.getAnnotation(Entity.class) != null)
		{
			for (Field field : aType.getDeclaredFields())
			{
				if ((field.getModifiers() & (Modifier.TRANSIENT | Modifier.STATIC | Modifier.FINAL)) == 0)
				{
					if (field.getAnnotation(Column.class) != null || field.getAnnotation(Id.class) != null || field.getAnnotation(Discriminator.class) != null)
					{
						field.setAccessible(true);

						aOutput.add(field);
					}
				}
			}
		}
		else
		{
			for (Field field : aType.getDeclaredFields())
			{
				if ((field.getModifiers() & (Modifier.TRANSIENT | Modifier.STATIC | Modifier.FINAL)) == 0)
				{
					field.setAccessible(true);

					aOutput.add(field);
				}
			}
		}

		Class<?> sup = aType.getSuperclass();

		if (sup != Object.class)
		{
			getDeclaredFields(sup, aOutput);
		}

		return aOutput;
	}
}

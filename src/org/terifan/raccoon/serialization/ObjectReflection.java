package org.terifan.raccoon.serialization;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;


class ObjectReflection
{
	public static ArrayList<Field> getDeclaredFields(Class<?> aType)
	{
		return getDeclaredFields(aType, new ArrayList<>());
	}


	private static ArrayList<Field> getDeclaredFields(Class<?> aType, ArrayList<Field> aOutput)
	{
		for (Field field : aType.getDeclaredFields())
		{
			if ((field.getModifiers() & (Modifier.TRANSIENT | Modifier.STATIC | Modifier.FINAL)) == 0)
			{
				field.setAccessible(true);

				aOutput.add(field);
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

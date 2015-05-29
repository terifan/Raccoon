package org.terifan.raccoon.serialization;

import java.io.DataInput;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;


class MapReader
{
	static Object readMap(Field aField, Object aObject, FieldType aTypeInfo, DataInput aDataInput) throws IOException, IllegalAccessException
	{
		if (aDataInput.readBoolean())
		{
			return null;
		}

		FieldType keyComponentType = aTypeInfo.componentType[0];
		FieldType valueComponentType = aTypeInfo.componentType[1];

		Object keys = ArrayReader.readArray(keyComponentType, 1, keyComponentType.depth + 1, aDataInput);
		Object values = ArrayReader.readArray(valueComponentType, 1, valueComponentType.depth + 1, aDataInput);

		Map value = createMapInstance(aField, aObject, aTypeInfo);

		for (int i = 0, sz = Array.getLength(keys); i < sz; i++)
		{
			value.put(Array.get(keys, i), Array.get(values, i));
		}

		return value;
	}


	private static Map createMapInstance(Field aField, Object aObject, FieldType aTypeInfo) throws IllegalArgumentException, IllegalAccessException
	{
		Map value = aField == null ? null : (Map)aField.get(aObject);

		if (value == null && !aTypeInfo.type.isInterface())
		{
			try
			{
				value = (Map)aTypeInfo.type.newInstance();
			}
			catch (InstantiationException e)
			{
			}
		}

		if (value == null)
		{
			value = new HashMap();
		}

		return value;
	}
}

package org.terifan.raccoon.serialization;

import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;


class MapReader
{
	static Object readMap(Field aField, Object aObject, FieldType aTypeInfo, DataInputStream aInputStream) throws IOException, IllegalAccessException
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

		FieldType keyComponentType = aTypeInfo.componentType[0];
		FieldType valueComponentType = aTypeInfo.componentType[1];

		Object keys = ArrayReader.readArray(keyComponentType, 1, keyComponentType.depth + 1, aInputStream);
		Object values = ArrayReader.readArray(valueComponentType, 1, valueComponentType.depth + 1, aInputStream);

		for (int i = 0, sz = Array.getLength(keys); i < sz; i++)
		{
			value.put(Array.get(keys, i), Array.get(values, i));
		}

		return value;
	}
}

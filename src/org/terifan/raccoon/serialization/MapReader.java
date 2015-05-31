package org.terifan.raccoon.serialization;

import java.io.DataInput;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Map;


class MapReader
{
	static Map readMap(FieldType aTypeInfo, DataInput aDataInput, Map aOutput) throws IOException, IllegalAccessException
	{
		if (aDataInput.readBoolean())
		{
			return null;
		}

		FieldType keyComponentType = aTypeInfo.componentType[0];
		FieldType valueComponentType = aTypeInfo.componentType[1];

		Object keys = ArrayReader.readArray(keyComponentType, 1, keyComponentType.depth + 1, aDataInput);
		Object values = ArrayReader.readArray(valueComponentType, 1, valueComponentType.depth + 1, aDataInput);

		for (int i = 0, sz = Array.getLength(keys); i < sz; i++)
		{
			aOutput.put(Array.get(keys, i), Array.get(values, i));
		}

		return aOutput;
	}
}

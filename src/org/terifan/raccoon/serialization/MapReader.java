package org.terifan.raccoon.serialization;

import java.lang.reflect.Array;
import java.util.Map;
import org.terifan.raccoon.util.ByteArrayBuffer;


class MapReader
{
	static Map readMap(FieldType aFieldType, ByteArrayBuffer aDataInput, Map aOutput) throws IllegalAccessException
	{
		if (aDataInput.readBit() == 1)
		{
			return null;
		}

		FieldType keyComponentType = aFieldType.componentType[0];
		FieldType valueComponentType = aFieldType.componentType[1];

		Object keys = ArrayReader.readArray(keyComponentType, 1, keyComponentType.depth + 1, aDataInput);
		Object values = ArrayReader.readArray(valueComponentType, 1, valueComponentType.depth + 1, aDataInput);

		for (int i = 0, sz = Array.getLength(keys); i < sz; i++)
		{
			aOutput.put(Array.get(keys, i), Array.get(values, i));
		}

		return aOutput;
	}
}

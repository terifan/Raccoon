package org.terifan.raccoon.serialization.old;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Map;
import org.terifan.raccoon.util.ByteArrayBuffer;


class MapReader
{
	static Map readMap(FieldType aFieldType, ByteArrayBuffer aDataInput, Map aOutput) throws IOException
	{
		if (aDataInput.read() != 0)
		{
			return null;
		}

		FieldType keyComponentType = aFieldType.getComponentType()[0];
		FieldType valueComponentType = aFieldType.getComponentType()[1];

		Object keys = ArrayReader.readArray(keyComponentType, 1, keyComponentType.getDepth() + 1, aDataInput);
		Object values = ArrayReader.readArray(valueComponentType, 1, valueComponentType.getDepth() + 1, aDataInput);

		for (int i = 0, sz = Array.getLength(keys); i < sz; i++)
		{
			aOutput.put(Array.get(keys, i), Array.get(values, i));
		}

		return aOutput;
	}
}

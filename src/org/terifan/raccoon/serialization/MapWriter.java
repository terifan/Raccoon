package org.terifan.raccoon.serialization;

import java.util.Map;
import org.terifan.raccoon.util.ByteArrayBuffer;


class MapWriter
{
	static void writeMap(FieldType aFieldType, Map aValue, ByteArrayBuffer aDataOutput) throws IllegalAccessException
	{
		if (aValue == null)
		{
			aDataOutput.writeBit(1);
			return;
		}

		aDataOutput.writeBit(0);

		FieldType keyComponentType = aFieldType.componentType[0];
		FieldType valueComponentType = aFieldType.componentType[1];

		ArrayWriter.writeArray(keyComponentType, ((Map)aValue).keySet().toArray(), 1, keyComponentType.depth + 1, aDataOutput);
		ArrayWriter.writeArray(valueComponentType, ((Map)aValue).values().toArray(), 1, valueComponentType.depth + 1, aDataOutput);
	}
}

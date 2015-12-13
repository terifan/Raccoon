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

		FieldType keyComponentType = aFieldType.getComponentType()[0];
		FieldType valueComponentType = aFieldType.getComponentType()[1];

		ArrayWriter.writeArray(keyComponentType, aValue.keySet().toArray(), 1, keyComponentType.getDepth() + 1, aDataOutput);
		ArrayWriter.writeArray(valueComponentType, aValue.values().toArray(), 1, valueComponentType.getDepth() + 1, aDataOutput);
	}
}

package org.terifan.raccoon.serialization;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;


class MapWriter
{
	static void writeMap(FieldType aFieldType, Map aValue, DataOutput aDataOutput) throws IOException, IllegalAccessException
	{
		if (aValue == null)
		{
			aDataOutput.writeBoolean(true);
			return;
		}

		aDataOutput.writeBoolean(false);

		FieldType keyComponentType = aFieldType.componentType[0];
		FieldType valueComponentType = aFieldType.componentType[1];

		ArrayWriter.writeArray(keyComponentType, ((Map)aValue).keySet().toArray(), 1, keyComponentType.depth + 1, aDataOutput);
		ArrayWriter.writeArray(valueComponentType, ((Map)aValue).values().toArray(), 1, valueComponentType.depth + 1, aDataOutput);
	}
}

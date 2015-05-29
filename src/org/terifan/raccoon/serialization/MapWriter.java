package org.terifan.raccoon.serialization;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;


class MapWriter
{
	static void writeMap(FieldType typeInfo, Map value, DataOutput aDataOutput) throws IOException, IllegalAccessException
	{
		if (value == null)
		{
			aDataOutput.writeBoolean(true);
			return;
		}

		aDataOutput.writeBoolean(false);

		FieldType keyComponentType = typeInfo.componentType[0];
		FieldType valueComponentType = typeInfo.componentType[1];

		ArrayWriter.writeArray(keyComponentType, ((Map)value).keySet().toArray(), 1, keyComponentType.depth + 1, aDataOutput);
		ArrayWriter.writeArray(valueComponentType, ((Map)value).values().toArray(), 1, valueComponentType.depth + 1, aDataOutput);
	}
}

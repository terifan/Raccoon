package org.terifan.raccoon.serialization;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;


class MapWriter
{
	static void writeMap(FieldType typeInfo, Map value, DataOutputStream out) throws IOException, IllegalAccessException
	{
		FieldType keyComponentType = typeInfo.componentType[0];
		FieldType valueComponentType = typeInfo.componentType[1];

		ArrayWriter.writeArray(keyComponentType, ((Map)value).keySet().toArray(), 1, keyComponentType.depth + 1, out);
		ArrayWriter.writeArray(valueComponentType, ((Map)value).values().toArray(), 1, valueComponentType.depth + 1, out);
	}
}

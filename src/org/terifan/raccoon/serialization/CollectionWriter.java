package org.terifan.raccoon.serialization;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;


class CollectionWriter
{
	static void writeCollection(FieldType typeInfo, Collection value, DataOutputStream out) throws IOException, IllegalAccessException
	{
		FieldType componentType = typeInfo.componentType[0];
		ArrayWriter.writeArray(componentType, value.toArray(), 1, componentType.depth + 1, out);
	}
}

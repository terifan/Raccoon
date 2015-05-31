package org.terifan.raccoon.serialization;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;


class CollectionWriter
{
	static void writeCollection(FieldType aFieldType, Collection aCollection, DataOutput aDataOutput) throws IOException, IllegalAccessException
	{
		if (aCollection == null)
		{
			aDataOutput.writeBoolean(true);
			return;
		}

		aDataOutput.writeBoolean(false);

		FieldType componentType = aFieldType.componentType[0];
		ArrayWriter.writeArray(componentType, aCollection.toArray(), 1, componentType.depth + 1, aDataOutput);
	}
}

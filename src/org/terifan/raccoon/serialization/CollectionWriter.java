package org.terifan.raccoon.serialization;

import java.util.Collection;
import org.terifan.raccoon.util.ByteArrayBuffer;


class CollectionWriter
{
	static void writeCollection(FieldType aFieldType, Collection aCollection, ByteArrayBuffer aDataOutput) throws IllegalAccessException
	{
		if (aCollection == null)
		{
			aDataOutput.writeBit(1);
			return;
		}

		aDataOutput.writeBit(0);

		FieldType componentType = aFieldType.componentType[0];
		ArrayWriter.writeArray(componentType, aCollection.toArray(), 1, componentType.depth + 1, aDataOutput);
	}
}

package org.terifan.raccoon.serialization.old;

import java.util.Collection;
import org.terifan.raccoon.util.ByteArrayBuffer;


class CollectionWriter
{
	static void writeCollection(FieldType aFieldType, Collection aCollection, ByteArrayBuffer aDataOutput) throws IllegalAccessException
	{
		if (aCollection == null)
		{
			aDataOutput.write(1);
			return;
		}

		aDataOutput.write(0);

		FieldType componentType = aFieldType.getComponentType()[0];
		ArrayWriter.writeArray(componentType, aCollection.toArray(), 1, componentType.getDepth() + 1, aDataOutput);
	}
}

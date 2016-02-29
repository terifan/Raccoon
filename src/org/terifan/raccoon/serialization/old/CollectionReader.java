package org.terifan.raccoon.serialization.old;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Collection;
import org.terifan.raccoon.util.ByteArrayBuffer;


class CollectionReader
{
	static Collection readCollection(FieldType aFieldType, ByteArrayBuffer aDataInput, Collection aOutput) throws IOException
	{
		if (aDataInput.read() != 0)
		{
			return null;
		}

		FieldType componentType = aFieldType.getComponentType()[0];
		Object values = ArrayReader.readArray(componentType, 1, componentType.getDepth() + 1, aDataInput);

		for (int i = 0, sz = Array.getLength(values); i < sz; i++)
		{
			aOutput.add(Array.get(values, i));
		}

		return aOutput;
	}
}

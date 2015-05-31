package org.terifan.raccoon.serialization;

import java.io.DataInput;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Collection;


class CollectionReader
{
	static Collection readCollection(FieldType aTypeInfo, DataInput aDataInput, Collection aOutput) throws IOException, IllegalAccessException
	{
		if (aDataInput.readBoolean())
		{
			return null;
		}

		FieldType componentType = aTypeInfo.componentType[0];
		Object values = ArrayReader.readArray(componentType, 1, componentType.depth + 1, aDataInput);

		for (int i = 0, sz = Array.getLength(values); i < sz; i++)
		{
			aOutput.add(Array.get(values, i));
		}

		return aOutput;
	}
}

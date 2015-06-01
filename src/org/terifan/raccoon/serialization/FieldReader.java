package org.terifan.raccoon.serialization;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;


class FieldReader
{
	static Object readField(FieldType aFieldType, ByteArrayBuffer aDataInput) throws IOException, IllegalAccessException
	{
		Log.v("decode " + aFieldType);

		switch (aFieldType.format)
		{
			case VALUE:
				if (aFieldType.nullable && aDataInput.readBit() == 1)
				{
					return null;
				}

				return ValueReader.readValue(aFieldType.type, aDataInput);
			case ARRAY:
				return ArrayReader.readArray(aFieldType, 1, aFieldType.depth, aDataInput);
			case LIST:
				return CollectionReader.readCollection(aFieldType, aDataInput, new ArrayList<>());
			case SET:
				return CollectionReader.readCollection(aFieldType, aDataInput, new HashSet<>());
			case MAP:
				return MapReader.readMap(aFieldType, aDataInput, new HashMap<>());
			default:
				throw new Error();
		}
	}
}

package org.terifan.raccoon.serialization;

import java.io.DataInput;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import org.terifan.raccoon.util.Log;


class FieldReader
{
	static Object readField(FieldType aTypeInfo, DataInput aDataInput) throws IOException, IllegalAccessException
	{
		Log.v("decode " + aTypeInfo);

		switch (aTypeInfo.format)
		{
			case VALUE:
				return ValueReader.readValue(aTypeInfo, aDataInput);
			case ARRAY:
				return ArrayReader.readArray(aTypeInfo, 1, aTypeInfo.depth, aDataInput);
			case LIST:
				return CollectionReader.readCollection(aTypeInfo, aDataInput, new ArrayList<>());
			case SET:
				return CollectionReader.readCollection(aTypeInfo, aDataInput, new HashSet<>());
			case MAP:
				return MapReader.readMap(aTypeInfo, aDataInput, new HashMap<>());
			default:
				throw new Error();
		}
	}
}

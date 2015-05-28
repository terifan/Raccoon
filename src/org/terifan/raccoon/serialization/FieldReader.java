package org.terifan.raccoon.serialization;

import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import org.terifan.raccoon.util.Log;


class FieldReader
{
	static void readField(FieldType aTypeInfo, DataInputStream aIn, Field aField, Object aObject) throws IOException, IllegalAccessException
	{
		Object value;

		Log.v("decode " + aTypeInfo);

		switch (aTypeInfo.code)
		{
			case 1:
				value = ValueReader.readValue(aTypeInfo.type, aIn);
				break;
			case 2:
				value = ArrayReader.readArray(aTypeInfo, 1, aTypeInfo.depth, aIn);
				break;
			case 3:
				value = CollectionReader.readCollection(aField, aObject, aTypeInfo, aIn);
				break;
			case 4:
				value = MapReader.readMap(aField, aObject, aTypeInfo, aIn);
				break;
			default:
				throw new Error();
		}

		if (aField != null)
		{
			aField.set(aObject, value);
		}
	}
}

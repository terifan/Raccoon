package org.terifan.raccoon.serialization;

import java.io.DataInput;
import java.io.IOException;
import java.lang.reflect.Field;
import org.terifan.raccoon.util.Log;


class FieldReader
{
	static void readField(FieldType aTypeInfo, DataInput aDataInput, Field aField, Object aObject) throws IOException, IllegalAccessException
	{
		Object value;

		Log.v("decode " + aTypeInfo);

		switch (aTypeInfo.format)
		{
			case VALUE:
				value = ValueReader.readValue(aTypeInfo, aDataInput);
				break;
			case ARRAY:
				value = ArrayReader.readArray(aTypeInfo, 1, aTypeInfo.depth, aDataInput);
				break;
			case LIST:
				value = CollectionReader.readCollection(aField, aObject, aTypeInfo, aDataInput);
				break;
			case SET:
				value = CollectionReader.readCollection(aField, aObject, aTypeInfo, aDataInput);
				break;
			case MAP:
				value = MapReader.readMap(aField, aObject, aTypeInfo, aDataInput);
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

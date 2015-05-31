package org.terifan.raccoon.serialization;

import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.terifan.raccoon.util.Log;


class FieldWriter
{
	static void writeField(FieldType aTypeInfo, DataOutput aDataOutput, Object aValue) throws IOException, IllegalAccessException
	{
		Log.v("encode " + aTypeInfo);

		switch (aTypeInfo.format)
		{
			case ARRAY:
				ArrayWriter.writeArray(aTypeInfo, aValue, 1, aTypeInfo.depth, aDataOutput);
				break;
			case LIST:
				CollectionWriter.writeCollection(aTypeInfo, (List)aValue, aDataOutput);
				break;
			case SET:
				CollectionWriter.writeCollection(aTypeInfo, (Set)aValue, aDataOutput);
				break;
			case MAP:
				MapWriter.writeMap(aTypeInfo, (Map)aValue, aDataOutput);
				break;
			case VALUE:
				ValueWriter.writeValue(aTypeInfo.primitive, aValue, aDataOutput);
				break;
			default:
				throw new IllegalStateException();
		}
	}
}
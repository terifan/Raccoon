package org.terifan.raccoon.serialization;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;


class FieldWriter
{
	static void writeField(FieldType aFieldType, ByteArrayBuffer aDataOutput, Object aValue) throws IOException, IllegalAccessException
	{
		Log.v("encode " + aFieldType);

		switch (aFieldType.format)
		{
			case ARRAY:
				ArrayWriter.writeArray(aFieldType, aValue, 1, aFieldType.depth, aDataOutput);
				break;
			case LIST:
				CollectionWriter.writeCollection(aFieldType, (List)aValue, aDataOutput);
				break;
			case SET:
				CollectionWriter.writeCollection(aFieldType, (Set)aValue, aDataOutput);
				break;
			case MAP:
				MapWriter.writeMap(aFieldType, (Map)aValue, aDataOutput);
				break;
			case VALUE:
				ValueWriter.writeValue(aFieldType.nullable, aValue, aDataOutput);
				break;
			default:
				throw new IllegalStateException();
		}
	}
}
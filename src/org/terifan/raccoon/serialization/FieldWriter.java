package org.terifan.raccoon.serialization;

import java.util.Collection;
import java.util.Map;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;


class FieldWriter
{
	static void writeField(FieldType aFieldType, ByteArrayBuffer aDataOutput, Object aValue) throws IllegalAccessException
	{
		Log.v("encode " + aFieldType);

		switch (aFieldType.getFormat())
		{
			case ARRAY:
				ArrayWriter.writeArray(aFieldType, aValue, 1, aFieldType.getDepth(), aDataOutput);
				break;
			case LIST:
			case SET:
				CollectionWriter.writeCollection(aFieldType, (Collection)aValue, aDataOutput);
				break;
			case MAP:
				MapWriter.writeMap(aFieldType, (Map)aValue, aDataOutput);
				break;
			case VALUE:
				if (aFieldType.isNullable())
				{
					if (aValue == null)
					{
						aDataOutput.write(1);
						break;
					}
					aDataOutput.write(0);
				}
				ValueWriter.writeValue(aValue, aDataOutput);
				break;
			default:
				throw new IllegalStateException();
		}
	}
}
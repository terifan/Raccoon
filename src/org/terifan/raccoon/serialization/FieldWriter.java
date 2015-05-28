package org.terifan.raccoon.serialization;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.terifan.raccoon.util.Log;


class FieldWriter
{
	static void writeField(FieldType aTypeInfo, DataOutputStream aOut, Object aValue) throws IOException, IllegalAccessException
	{
		Log.v("encode " + aTypeInfo);

		if (aTypeInfo.array)
		{
			ArrayWriter.writeArray(aTypeInfo, aValue, 1, aTypeInfo.depth, aOut);
		}
		else if (List.class.isAssignableFrom(aTypeInfo.type) || Set.class.isAssignableFrom(aTypeInfo.type))
		{
			CollectionWriter.writeCollection(aTypeInfo, (Collection)aValue, aOut);
		}
		else if (Map.class.isAssignableFrom(aTypeInfo.type))
		{
			MapWriter.writeMap(aTypeInfo, (Map)aValue, aOut);
		}
		else
		{
			ValueWriter.writeValue(aValue, aOut);
		}
	}
}

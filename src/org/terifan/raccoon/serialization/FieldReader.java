package org.terifan.raccoon.serialization;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.terifan.raccoon.DatabaseException;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;


class FieldReader
{
	static Object readField(FieldType aFieldType, ByteArrayBuffer aDataInput, Field aField) throws IOException
	{
		Log.v("decode %s", aFieldType);

		try
		{
			switch (aFieldType.getFormat())
			{
				case VALUE:
					if (aFieldType.isNullable() && aDataInput.read() != 0)
					{
						return null;
					}

					return ValueReader.readValue(aFieldType.getType(), aDataInput);
				case ARRAY:
					return ArrayReader.readArray(aFieldType, 1, aFieldType.getDepth(), aDataInput);
				case LIST:
					return CollectionReader.readCollection(aFieldType, aDataInput, newInstance(ArrayList.class, List.class, aField));
				case SET:
					return CollectionReader.readCollection(aFieldType, aDataInput, newInstance(HashSet.class, Set.class, aField));
				case MAP:
					return MapReader.readMap(aFieldType, aDataInput, newInstance(HashMap.class, Map.class, aField));
				default:
					throw new Error();
			}
		}
		catch (InstantiationException | IllegalAccessException e)
		{
			throw new IOException(e);
		}
	}


	private static <T> T newInstance(Class<T> aType, Class aParentType, Field aField) throws DatabaseException, InstantiationException, IllegalAccessException
	{
		if (aField != null)
		{
			Class type = aField.getType();

			if (aParentType.isAssignableFrom(type) && !type.isInterface())
			{
				try
				{
					return (T)type.newInstance();
				}
				catch (InstantiationException | IllegalAccessException e)
				{
					throw new DatabaseException("Failed to create new object instance while deserializing field " + aField, e);
				}
			}
		}

		return aType.newInstance();
	}
}

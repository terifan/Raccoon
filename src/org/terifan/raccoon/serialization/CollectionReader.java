package org.terifan.raccoon.serialization;

import java.io.DataInput;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;


class CollectionReader
{
	static Object readCollection(Field aField, Object aObject, FieldType aTypeInfo, DataInput aDataInput) throws IOException, IllegalAccessException
	{
		if (aDataInput.readBoolean())
		{
			return null;
		}

		Collection value = aField == null ? null : (Collection)aField.get(aObject);

		if (value == null && !aTypeInfo.type.isInterface())
		{
			try
			{
				value = (Collection)aTypeInfo.type.newInstance();
			}
			catch (InstantiationException e)
			{
			}
		}
		if (value == null)
		{
			if (List.class.isAssignableFrom(aTypeInfo.type))
			{
				value = new ArrayList();
			}
			else
			{
				value = new HashSet();
			}
		}

		FieldType componentType = aTypeInfo.componentType[0];
		Object values = ArrayReader.readArray(componentType, 1, componentType.depth + 1, aDataInput);

		for (int i = 0, sz = Array.getLength(values); i < sz; i++)
		{
			value.add(Array.get(values, i));
		}

		return value;
	}
}

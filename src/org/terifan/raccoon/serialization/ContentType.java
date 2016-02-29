package org.terifan.raccoon.serialization;

import java.util.HashMap;


public enum ContentType
{
	BOOLEAN,
	BYTE,
	SHORT,
	CHAR,
	INT,
	LONG,
	FLOAT,
	DOUBLE,
	STRING,
	DATE,
	OBJECT;

	public final static HashMap<Class,ContentType> types = new HashMap<>();
	public final static HashMap<Class,ContentType> classTypes = new HashMap<>();

	static
	{
		types.put(Boolean.TYPE, ContentType.BOOLEAN);
		types.put(Byte.TYPE, ContentType.BYTE);
		types.put(Short.TYPE, ContentType.SHORT);
		types.put(Character.TYPE, ContentType.CHAR);
		types.put(Integer.TYPE, ContentType.INT);
		types.put(Long.TYPE, ContentType.LONG);
		types.put(Float.TYPE, ContentType.FLOAT);
		types.put(Double.TYPE, ContentType.DOUBLE);

		classTypes.put(Boolean.class, ContentType.BOOLEAN);
		classTypes.put(Byte.class, ContentType.BYTE);
		classTypes.put(Short.class, ContentType.SHORT);
		classTypes.put(Character.class, ContentType.CHAR);
		classTypes.put(Integer.class, ContentType.INT);
		classTypes.put(Long.class, ContentType.LONG);
		classTypes.put(Float.class, ContentType.FLOAT);
		classTypes.put(Double.class, ContentType.DOUBLE);
	}
}

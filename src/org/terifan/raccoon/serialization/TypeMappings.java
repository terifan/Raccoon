package org.terifan.raccoon.serialization;

import java.util.Date;
import java.util.HashMap;


class TypeMappings
{
	protected final static HashMap<ContentType, Class> TYPE_VALUES = new HashMap<>();
	protected final static HashMap<ContentType, Class> TYPE_CLASSES = new HashMap<>();
	protected final static HashMap<Class, ContentType> VALUE_TYPES = new HashMap<>();
	protected final static HashMap<Class, ContentType> CLASS_TYPES = new HashMap<>();


	static
	{
		TYPE_VALUES.put(ContentType.BOOLEAN, Boolean.TYPE);
		TYPE_VALUES.put(ContentType.BYTE, Byte.TYPE);
		TYPE_VALUES.put(ContentType.SHORT, Short.TYPE);
		TYPE_VALUES.put(ContentType.CHAR, Character.TYPE);
		TYPE_VALUES.put(ContentType.INT, Integer.TYPE);
		TYPE_VALUES.put(ContentType.LONG, Long.TYPE);
		TYPE_VALUES.put(ContentType.FLOAT, Float.TYPE);
		TYPE_VALUES.put(ContentType.DOUBLE, Double.TYPE);
		TYPE_VALUES.put(ContentType.STRING, String.class);
		TYPE_VALUES.put(ContentType.DATE, Date.class);

		TYPE_CLASSES.put(ContentType.BOOLEAN, Boolean.class);
		TYPE_CLASSES.put(ContentType.BYTE, Byte.class);
		TYPE_CLASSES.put(ContentType.SHORT, Short.class);
		TYPE_CLASSES.put(ContentType.CHAR, Character.class);
		TYPE_CLASSES.put(ContentType.INT, Integer.class);
		TYPE_CLASSES.put(ContentType.LONG, Long.class);
		TYPE_CLASSES.put(ContentType.FLOAT, Float.class);
		TYPE_CLASSES.put(ContentType.DOUBLE, Double.class);
		TYPE_CLASSES.put(ContentType.STRING, String.class);
		TYPE_CLASSES.put(ContentType.DATE, Date.class);

		VALUE_TYPES.put(Boolean.TYPE, ContentType.BOOLEAN);
		VALUE_TYPES.put(Byte.TYPE, ContentType.BYTE);
		VALUE_TYPES.put(Short.TYPE, ContentType.SHORT);
		VALUE_TYPES.put(Character.TYPE, ContentType.CHAR);
		VALUE_TYPES.put(Integer.TYPE, ContentType.INT);
		VALUE_TYPES.put(Long.TYPE, ContentType.LONG);
		VALUE_TYPES.put(Float.TYPE, ContentType.FLOAT);
		VALUE_TYPES.put(Double.TYPE, ContentType.DOUBLE);

		CLASS_TYPES.put(Boolean.class, ContentType.BOOLEAN);
		CLASS_TYPES.put(Byte.class, ContentType.BYTE);
		CLASS_TYPES.put(Short.class, ContentType.SHORT);
		CLASS_TYPES.put(Character.class, ContentType.CHAR);
		CLASS_TYPES.put(Integer.class, ContentType.INT);
		CLASS_TYPES.put(Long.class, ContentType.LONG);
		CLASS_TYPES.put(Float.class, ContentType.FLOAT);
		CLASS_TYPES.put(Double.class, ContentType.DOUBLE);
	}
}

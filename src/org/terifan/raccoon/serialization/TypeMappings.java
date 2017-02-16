package org.terifan.raccoon.serialization;

import java.util.Date;
import java.util.HashMap;


class TypeMappings
{
	protected final static HashMap<FieldType, Class> TYPE_VALUES = new HashMap<>();
	protected final static HashMap<FieldType, Class> TYPE_CLASSES = new HashMap<>();
	protected final static HashMap<Class, FieldType> VALUE_TYPES = new HashMap<>();
	protected final static HashMap<Class, FieldType> CLASS_TYPES = new HashMap<>();


	static
	{
		TYPE_VALUES.put(FieldType.BOOLEAN, Boolean.TYPE);
		TYPE_VALUES.put(FieldType.BYTE, Byte.TYPE);
		TYPE_VALUES.put(FieldType.SHORT, Short.TYPE);
		TYPE_VALUES.put(FieldType.CHAR, Character.TYPE);
		TYPE_VALUES.put(FieldType.INT, Integer.TYPE);
		TYPE_VALUES.put(FieldType.LONG, Long.TYPE);
		TYPE_VALUES.put(FieldType.FLOAT, Float.TYPE);
		TYPE_VALUES.put(FieldType.DOUBLE, Double.TYPE);
		TYPE_VALUES.put(FieldType.STRING, String.class);
		TYPE_VALUES.put(FieldType.DATE, Date.class);

		TYPE_CLASSES.put(FieldType.BOOLEAN, Boolean.class);
		TYPE_CLASSES.put(FieldType.BYTE, Byte.class);
		TYPE_CLASSES.put(FieldType.SHORT, Short.class);
		TYPE_CLASSES.put(FieldType.CHAR, Character.class);
		TYPE_CLASSES.put(FieldType.INT, Integer.class);
		TYPE_CLASSES.put(FieldType.LONG, Long.class);
		TYPE_CLASSES.put(FieldType.FLOAT, Float.class);
		TYPE_CLASSES.put(FieldType.DOUBLE, Double.class);
		TYPE_CLASSES.put(FieldType.STRING, String.class);
		TYPE_CLASSES.put(FieldType.DATE, Date.class);

		VALUE_TYPES.put(Boolean.TYPE, FieldType.BOOLEAN);
		VALUE_TYPES.put(Byte.TYPE, FieldType.BYTE);
		VALUE_TYPES.put(Short.TYPE, FieldType.SHORT);
		VALUE_TYPES.put(Character.TYPE, FieldType.CHAR);
		VALUE_TYPES.put(Integer.TYPE, FieldType.INT);
		VALUE_TYPES.put(Long.TYPE, FieldType.LONG);
		VALUE_TYPES.put(Float.TYPE, FieldType.FLOAT);
		VALUE_TYPES.put(Double.TYPE, FieldType.DOUBLE);

		CLASS_TYPES.put(Boolean.class, FieldType.BOOLEAN);
		CLASS_TYPES.put(Byte.class, FieldType.BYTE);
		CLASS_TYPES.put(Short.class, FieldType.SHORT);
		CLASS_TYPES.put(Character.class, FieldType.CHAR);
		CLASS_TYPES.put(Integer.class, FieldType.INT);
		CLASS_TYPES.put(Long.class, FieldType.LONG);
		CLASS_TYPES.put(Float.class, FieldType.FLOAT);
		CLASS_TYPES.put(Double.class, FieldType.DOUBLE);
	}
}

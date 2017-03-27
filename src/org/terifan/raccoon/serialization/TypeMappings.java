package org.terifan.raccoon.serialization;

import java.util.Date;
import java.util.HashMap;


class TypeMappings
{
	protected final static HashMap<ValueType, Class> TYPE_VALUES = new HashMap<>();
	protected final static HashMap<ValueType, Class> TYPE_CLASSES = new HashMap<>();
	protected final static HashMap<Class, ValueType> VALUE_TYPES = new HashMap<>();
	protected final static HashMap<Class, ValueType> CLASS_TYPES = new HashMap<>();


	static
	{
		TYPE_VALUES.put(ValueType.BOOLEAN, Boolean.TYPE);
		TYPE_VALUES.put(ValueType.BYTE, Byte.TYPE);
		TYPE_VALUES.put(ValueType.SHORT, Short.TYPE);
		TYPE_VALUES.put(ValueType.CHAR, Character.TYPE);
		TYPE_VALUES.put(ValueType.INT, Integer.TYPE);
		TYPE_VALUES.put(ValueType.LONG, Long.TYPE);
		TYPE_VALUES.put(ValueType.FLOAT, Float.TYPE);
		TYPE_VALUES.put(ValueType.DOUBLE, Double.TYPE);
		TYPE_VALUES.put(ValueType.STRING, String.class);
		TYPE_VALUES.put(ValueType.DATE, Date.class);

		TYPE_CLASSES.put(ValueType.BOOLEAN, Boolean.class);
		TYPE_CLASSES.put(ValueType.BYTE, Byte.class);
		TYPE_CLASSES.put(ValueType.SHORT, Short.class);
		TYPE_CLASSES.put(ValueType.CHAR, Character.class);
		TYPE_CLASSES.put(ValueType.INT, Integer.class);
		TYPE_CLASSES.put(ValueType.LONG, Long.class);
		TYPE_CLASSES.put(ValueType.FLOAT, Float.class);
		TYPE_CLASSES.put(ValueType.DOUBLE, Double.class);
		TYPE_CLASSES.put(ValueType.STRING, String.class);
		TYPE_CLASSES.put(ValueType.DATE, Date.class);

		VALUE_TYPES.put(Boolean.TYPE, ValueType.BOOLEAN);
		VALUE_TYPES.put(Byte.TYPE, ValueType.BYTE);
		VALUE_TYPES.put(Short.TYPE, ValueType.SHORT);
		VALUE_TYPES.put(Character.TYPE, ValueType.CHAR);
		VALUE_TYPES.put(Integer.TYPE, ValueType.INT);
		VALUE_TYPES.put(Long.TYPE, ValueType.LONG);
		VALUE_TYPES.put(Float.TYPE, ValueType.FLOAT);
		VALUE_TYPES.put(Double.TYPE, ValueType.DOUBLE);

		CLASS_TYPES.put(Boolean.class, ValueType.BOOLEAN);
		CLASS_TYPES.put(Byte.class, ValueType.BYTE);
		CLASS_TYPES.put(Short.class, ValueType.SHORT);
		CLASS_TYPES.put(Character.class, ValueType.CHAR);
		CLASS_TYPES.put(Integer.class, ValueType.INT);
		CLASS_TYPES.put(Long.class, ValueType.LONG);
		CLASS_TYPES.put(Float.class, ValueType.FLOAT);
		CLASS_TYPES.put(Double.class, ValueType.DOUBLE);
	}
}

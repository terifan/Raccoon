package org.terifan.raccoon;

import java.lang.reflect.Field;


public interface MarshallerListener
{
	void valueDecoded(Object aObject, Field aField, String aFieldName, Object aValue);
}

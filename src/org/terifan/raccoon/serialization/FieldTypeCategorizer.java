package org.terifan.raccoon.serialization;

import java.lang.reflect.Field;


public interface FieldTypeCategorizer 
{
	int categorize(Field aField);
}

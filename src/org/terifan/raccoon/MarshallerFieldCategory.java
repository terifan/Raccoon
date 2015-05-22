package org.terifan.raccoon;

import java.lang.reflect.Field;


enum MarshallerFieldCategory
{
	KEY,
	DISCRIMINATOR,
	VALUE;


	static MarshallerFieldCategory classify(Field aField)
	{
		if (aField.getAnnotation(Key.class) != null)
		{
			return MarshallerFieldCategory.KEY;
		}
		if (aField.getAnnotation(Discriminator.class) != null)
		{
			return MarshallerFieldCategory.DISCRIMINATOR;
		}
		return MarshallerFieldCategory.VALUE;
	}
}

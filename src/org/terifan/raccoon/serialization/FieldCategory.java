package org.terifan.raccoon.serialization;


public enum FieldCategory
{
	KEY,
	DISCRIMINATOR,
	VALUE,

	/** used in marshaling to get both discriminator and value fields */
	DISCRIMINATOR_VALUE
}

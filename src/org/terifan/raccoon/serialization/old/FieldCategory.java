package org.terifan.raccoon.serialization.old;


public enum FieldCategory
{
	KEYS,
	DISCRIMINATORS,
	VALUES,

	/** used in marshaling to get both discriminator and value fields */
	DISCRIMINATOR_AND_VALUES
}
package org.terifan.raccoon.serialization;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import static org.terifan.raccoon.serialization.FieldCategory.*;


public interface FieldCategoryFilter
{
	HashSet<FieldCategory> ALL = new HashSet<>(Arrays.asList(DISCRIMINATOR, KEY, VALUE));
	HashSet<FieldCategory> DISCRIMINATORS_VALUES = new HashSet<>(Arrays.asList(DISCRIMINATOR, VALUE));
	List<FieldCategory> KEYS = Arrays.asList(KEY);
	List<FieldCategory> DISCRIMINATORS = Arrays.asList(DISCRIMINATOR);
	List<FieldCategory> VALUES = Arrays.asList(VALUE);
}

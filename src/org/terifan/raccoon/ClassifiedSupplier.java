package org.terifan.raccoon;


public interface ClassifiedSupplier<R>
{
	R get(Object aType);
}

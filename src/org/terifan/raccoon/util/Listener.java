package org.terifan.raccoon.util;

import java.io.IOException;


@FunctionalInterface
public interface Listener<T>
{
	void call(T aValue) throws IOException;
}

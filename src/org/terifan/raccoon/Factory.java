package org.terifan.raccoon;


public interface Factory<T>
{
	void newInstance(T aEntity);
}

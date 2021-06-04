package org.terifan.raccoon;


public class DiscriminatorType<T>
{
	private T instance;


	public DiscriminatorType(T aInstance)
	{
		instance = aInstance;
	}


	Class getType()
	{
		return instance.getClass();
	}


	T getInstance()
	{
		return instance;
	}
}

package org.terifan.raccoon;


public class DiscriminatorType<T>
{
	private T instance;


	public DiscriminatorType(T aInstance)
	{
		this.instance = aInstance;
	}
	
	
	Class getType()
	{
		return instance.getClass();
	}


	T newInstance()
	{
		return instance;
	}
}

package org.terifan.raccoon.util;


public class Result<T>
{
	private T mValue;


	public Result()
	{
	}


	public Result(T value)
	{
		mValue = value;
	}


	public T get()
	{
		return mValue;
	}


	public void set(T value)
	{
		mValue = value;
	}


	@Override
	public String toString()
	{
		return mValue == null ? "<null>" : mValue.toString();
	}
}

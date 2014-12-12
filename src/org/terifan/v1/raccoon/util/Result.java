package org.terifan.v1.raccoon.util;


/**
 * Method parameters can be wrapped using the Result class to enable multiple returns from a method.
 *
 * E.g.
 * <pre>
 * Result&lt;String&gt; s = new Result&lt;&gt;();
 * method(s);
 * System.out.println(s.get());
 *
 * void method(Result&lt;String&gt; s)
 * {
 *  s.set("Hello world");
 * }
 * </pre>
 *
 * @param <E>
 *   type of Object wrapped by this Result class
 */
public class Result<E>
{
	private E mValue;


	/**
	 * Create an uninitialized instance.
	 */
	public Result()
	{
	}


	/**
	 * Create an instance initialized with a value.
	 */
	public Result(E value)
	{
		mValue = value;
	}


	/**
	 * Gets the wrapped value.
	 */
	public E get()
	{
		return mValue;
	}


	/**
	 * Sets the wrapped value.
	 */
	public void set(E value)
	{
		mValue = value;
	}
}

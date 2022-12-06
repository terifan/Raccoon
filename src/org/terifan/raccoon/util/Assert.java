package org.terifan.raccoon.util;



public class Assert
{
	/**
	 * Throws exception if value is true.
	 */
	public static void fail(boolean aValue)
	{
		fail(aValue, "Test failed");
	}


	/**
	 * Throws exception if value is true.
	 */
	public static void fail(boolean aValue, String aErrorMessage, Object... aArguments)
	{
		if (aValue)
		{
			throw new IllegalArgumentException(String.format(aErrorMessage, aArguments));
		}
	}


	/**
	 * Throws exception if value is false.
	 */
	public static void verify(boolean aValue)
	{
		verify(aValue, "Value not verified");
	}


	/**
	 * Throws exception if value is false.
	 */
	public static void verify(boolean aValue, String aErrorMessage, Object... aArguments)
	{
		if (!aValue)
		{
			throw new IllegalArgumentException(String.format(aErrorMessage, aArguments));
		}
	}


	/**
	 * Throws exception if value is null.
	 */
	public static void notNull(Object aValue)
	{
		notNull(aValue, "Value is null");
	}


	/**
	 * Throws exception if value is null.
	 */
	public static void notNull(Object aValue, String aErrorMessage, Object... aArguments)
	{
		if (aValue == null)
		{
			throw new IllegalArgumentException(String.format(aErrorMessage, aArguments));
		}
	}


	/**
	 * Throws exception if value is empty or null.
	 */
	public static void notEmptyOrNull(Object aValue)
	{
		notEmptyOrNull(aValue, "Value is empty or null");
	}


	/**
	 * Throws exception if value is empty or null.
	 */
	public static void notEmptyOrNull(Object aValue, String aErrorMessage, Object... aArguments)
	{
		if (aValue == null || aValue.toString().isEmpty())
		{
			throw new IllegalArgumentException(String.format(aErrorMessage, aArguments));
		}
	}


	/**
	 * Throws exception if value don't match regular expression.
	 */
	public static void matches(Object aValue, String aPattern)
	{
		matches(aValue, aPattern, "Value don't match pattern");
	}


	/**
	 * Throws exception if value don't match regular expression.
	 */
	public static void matches(Object aValue, String aPattern, String aErrorMessage, Object... aArguments)
	{
		if (aValue == null || !aValue.toString().matches(aPattern))
		{
			throw new IllegalArgumentException(String.format(aErrorMessage, aArguments));
		}
	}


	public static void notEquals(Object aA, Object aB)
	{
		notEquals(aA, aB, null);
	}


	public static void notEquals(Object aA, Object aB, String aErrorMessage, Object... aArguments)
	{
		if (aA == aB)
		{
			throw new IllegalArgumentException(String.format(aErrorMessage, aArguments));
		}
	}
}
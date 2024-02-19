package org.terifan.raccoon.util;


public class Assert
{
	/**
	 * Throws exception.
	 */
	public static void fail()
	{
		throw new AssertionException("Assertion failed.");
	}


	/**
	 * Throws exception if parameter is false
	 */
	public static void assertTrue(boolean aBoolean)
	{
		if (!aBoolean)
		{
			throw new AssertionException("Not a true value");
		}
	}


	/**
	 * Throws exception if parameter is false
	 */
	public static void assertTrue(boolean aValue, String aErrorMessage, Object... aArguments)
	{
		if (!aValue)
		{
			throw new AssertionException(aErrorMessage, aArguments);
		}
	}


	/**
	 * Throws exception if parameter isn't false.
	 */
	public static void assertFalse(boolean aValue)
	{
		assertFalse(aValue, "%s", "Value not false");
	}


	/**
	 * Throws exception if parameter isn't false.
	 */
	public static void assertFalse(boolean aValue, String aErrorMessage, Object... aArguments)
	{
		if (aValue)
		{
			throw new AssertionException(aErrorMessage, aArguments);
		}
	}


	/**
	 * Throws exception if parameters do not equal
	 */
	public static void assertEquals(Object aValue, Object aExpected)
	{
		assertEquals(aValue, aExpected, "Values are not equal: {%s} {%s}", aValue, aExpected);
	}


	/**
	 * Throws exception if parameters do not equal
	 */
	public static void assertEquals(Object aValue, Object aExpected, String aErrorMessage, Object... aArguments)
	{
		if (!aValue.equals(aExpected))
		{
			throw new AssertionException(aErrorMessage, aArguments);
		}
	}


	/**
	 * Throws exception if parameter is null.
	 */
	public static void assertNotNull(Object aValue)
	{
		assertNotNull(aValue, "Value is null");
	}


	/**
	 * Throws exception if parameter is null.
	 */
	public static void assertNotNull(Object aValue, String aErrorMessage, Object... aArguments)
	{
		if (aValue == null)
		{
			throw new AssertionException(aErrorMessage, aArguments);
		}
	}


	/**
	 * Throws exception if parameter isn't null.
	 */
	public static void assertNull(Object aValue)
	{
		assertNull(aValue, "%s", "Value is null");
	}


	/**
	 * Throws exception if parameter isn't null.
	 */
	public static void assertNull(Object aValue, String aErrorMessage, Object... aArguments)
	{
		if (aValue != null)
		{
			throw new AssertionException(aErrorMessage, aArguments);
		}
	}


	/**
	 * Throws exception if parameter is empty or null.
	 */
	public static void assertNotEmptyOrNull(Object aValue)
	{
		assertNotEmptyOrNull(aValue, "Value is empty or null");
	}


	/**
	 * Throws exception if parameter is empty or null.
	 */
	public static void assertNotEmptyOrNull(Object aValue, String aErrorMessage, Object... aArguments)
	{
		if (aValue == null || aValue.toString().isEmpty())
		{
			throw new AssertionException(aErrorMessage, aArguments);
		}
	}


	/**
	 * Throws exception if parameter don't match regular expression.
	 */
	public static void assertRegexOrNull(Object aValue, String aPattern)
	{
		assertRegexOrNull(aValue, aPattern, "%s", "Value don't match pattern");
	}


	/**
	 * Throws exception if parameter don't match regular expression.
	 */
	public static void assertRegexOrNull(Object aValue, String aPattern, String aErrorMessage, Object... aArguments)
	{
		if (aValue != null && !aValue.toString().matches(aPattern))
		{
			throw new AssertionException(aErrorMessage, aArguments);
		}
	}


	/**
	 * Throws exception if parameter don't match regular expression.
	 */
	public static void assertRegex(Object aValue, String aPattern)
	{
		assertRegex(aValue, aPattern, "Value don't match pattern");
	}


	/**
	 * Throws exception if parameter don't match regular expression.
	 */
	public static void assertRegex(Object aValue, String aPattern, String aErrorMessage, Object... aArguments)
	{
		if (aValue == null || !aValue.toString().matches(aPattern))
		{
			throw new AssertionException(aErrorMessage, aArguments);
		}
	}


	/**
	 * Throws exception if parameters aren't the same object.
	 */
	public static void assertSame(Object aA, Object aB)
	{
		assertSame(aA, aB, null);
	}


	/**
	 * Throws exception if parameters aren't the same object.
	 */
	public static void assertSame(Object aA, Object aB, String aErrorMessage, Object... aArguments)
	{
		if (aA != aB)
		{
			throw new AssertionException(aErrorMessage, aArguments);
		}
	}


	/**
	 * Throws exception if the values aren't the same.
	 */
	public static void assertNotSame(Object aExpected, Object aActual)
	{
		assertNotSame(aExpected, aActual, "%s", "Provided objects are the same.");
	}


	/**
	 * Throws exception if the values aren't the same.
	 */
	public static void assertNotSame(Object aExpected, Object aActual, String aErrorMessage, Object... aArguments)
	{
		if (aExpected == aActual)
		{
			throw new AssertionException(aErrorMessage, aArguments);
		}
	}
}

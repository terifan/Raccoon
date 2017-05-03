package org.terifan.raccoon;

import java.lang.reflect.Field;
import java.util.Arrays;


public final class PerformanceCounters
{
	public final static int LAZY_WRITE_CACHE_READ = 0;
	public final static int LAZY_WRITE_CACHE_HIT = 1;
	public final static int LAZY_WRITE_CACHE_WRITE = 2;
	public final static int LAZY_WRITE_CACHE_FLUSH = 3;
	public final static int BLOCK_READ = 4;
	public final static int BLOCK_WRITE = 5;
	public final static int BLOCK_ALLOC = 6;
	public final static int BLOCK_FREE = 7;
	public final static int SPLIT_LEAF = 8;
	public final static int UPGRADE_HOLE_TO_LEAF = 9;
	public final static int REMOVE_VALUE = 10;
	public final static int PUT_VALUE_LEAF = 11;
	public final static int PUT_VALUE = 12;
	public final static int GET_VALUE = 13;
	public final static int POINTER_ENCODE = 14;
	public final static int POINTER_DECODE = 15;
	public final static int INDEX_NODE_CREATION = 16;
	public final static int LEAF_NODE_CREATION = 17;

	private final static long[] COUNTERS = new long[18];


	/**
	 * Increment a counter, called by Raccoon internals
	 *
	 * @return
	 *   always return true, this way the call can be made in an assert statement
	 */
	public static synchronized boolean increment(int aField)
	{
		COUNTERS[aField]++;
		return true;
	}


	public static synchronized boolean reset()
	{
		Arrays.fill(COUNTERS, 0);
		return true;
	}


	/**
	 * Return a comma separated key/value String with all values
	 */
	public static String print() throws IllegalAccessException
	{
		StringBuilder sb = new StringBuilder();

		for (Field f : PerformanceCounters.class.getDeclaredFields())
		{
			f.setAccessible(true);

			if (f.getType() == Integer.TYPE)
			{
				if (sb.length() > 0)
				{
					sb.append(", ");
				}

				sb.append(f.getName() + "=" + COUNTERS[(int)f.get(null)]);
			}
		}

		return sb.toString();
	}


	/**
	 * Return a tab separated String with all counter names
	 */
	public static String printNames() throws IllegalAccessException
	{
		StringBuilder sb = new StringBuilder();

		for (Field f : PerformanceCounters.class.getDeclaredFields())
		{
			f.setAccessible(true);

			if (f.getType() == Integer.TYPE)
			{
				if (sb.length() > 0)
				{
					sb.append("\t");
				}

				sb.append(f.getName());
			}
		}

		return sb.toString();
	}


	/**
	 * Return a tab separated String with all values
	 */
	public static String printValues() throws IllegalAccessException
	{
		StringBuilder sb = new StringBuilder();

		for (Field f : PerformanceCounters.class.getDeclaredFields())
		{
			f.setAccessible(true);

			if (f.getType() == Integer.TYPE)
			{
				if (sb.length() > 0)
				{
					sb.append("\t");
				}

				sb.append(COUNTERS[(int)f.get(null)]);
			}
		}

		return sb.toString();
	}
}

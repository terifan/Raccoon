package org.terifan.raccoon;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicLong;


public class PerformanceCounters
{
	public final static AtomicLong blockRead = new AtomicLong(0);
	public final static AtomicLong blockWrite = new AtomicLong(0);
	public final static AtomicLong blockAlloc = new AtomicLong(0);
	public final static AtomicLong blockFree = new AtomicLong(0);
	public final static AtomicLong splitLeaf = new AtomicLong(0);
	public final static AtomicLong upgradeHoleToLeaf = new AtomicLong(0);
	public final static AtomicLong removeValue = new AtomicLong(0);
	public final static AtomicLong putValueLeaf = new AtomicLong(0);
	public final static AtomicLong putValue = new AtomicLong(0);
	public final static AtomicLong getValue = new AtomicLong(0);
	public final static AtomicLong pointerEncode = new AtomicLong(0);
	public final static AtomicLong pointerDecode = new AtomicLong(0);
	public final static AtomicLong indexNodeCreation = new AtomicLong(0);
	public final static AtomicLong leafNodeCreation = new AtomicLong(0);

	
	public final static int LAZY_WRITE_CACHE_READ = 0;
	public final static int LAZY_WRITE_CACHE_HIT = 1;
	public final static int LAZY_WRITE_CACHE_WRITE = 2;
	public final static int LAZY_WRITE_CACHE_FLUSH = 3;
	
	private static long[] mCounters = new long[100];
	
	
	public static synchronized boolean increment(int aField)
	{
		return true;
	}
	
	
	public static String print() throws IllegalAccessException
	{
		StringBuilder sb = new StringBuilder();
		
		for (Field f : PerformanceCounters.class.getDeclaredFields())
		{
			f.setAccessible(true);
			
			if (sb.length() > 0)
			{
				sb.append(", ");
			}
			
			sb.append(f.getName()+"="+f.get(null));
		}
		
		return sb.toString();
	}
}

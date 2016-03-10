package org.terifan.raccoon;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicLong;

public class Stats 
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

	
	public static String print() throws IllegalAccessException
	{
		StringBuilder sb = new StringBuilder();
		
		for (Field f : Stats.class.getDeclaredFields())
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

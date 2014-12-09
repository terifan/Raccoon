package org.terifan.v1.util;

import java.text.SimpleDateFormat;
import java.util.Date;


public class Log
{
	private final static SimpleDateFormat mDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");


	public static void d(String aMessage, Object... aParams)
	{
		log("DEBUG", aMessage, aParams);
	}


	public static void i(String aMessage, Object... aParams)
	{
		log("INFO ", aMessage, aParams);
	}


	public static void w(String aMessage, Object... aParams)
	{
		log("WARN ", aMessage, aParams);
	}


	public static void e(String aMessage, Object... aParams)
	{
		log("ERROR", aMessage, aParams);
	}


	private static void log(String aLevel, String aMessage, Object... aParams)
	{
		System.out.printf(mDateFormat.format(new Date()) + " " + aLevel + " " + aMessage + "\n", aParams);
	}
}
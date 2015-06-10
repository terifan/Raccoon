package org.terifan.raccoon.serialization;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Random;
import org.terifan.raccoon.util.Log;


public class TestUtils
{
	public static Random rnd = new Random(0);


	public static boolean x()
	{
		return rnd.nextBoolean();
	}


	public static byte b()
	{
		return (byte)rnd.nextInt();
	}


	public static short s()
	{
		return (short)rnd.nextInt();
	}


	public static char c()
	{
		return (char)rnd.nextInt();
	}


	public static int i()
	{
		return rnd.nextInt();
	}


	public static long l()
	{
		return rnd.nextLong();
	}


	public static float f()
	{
		return (float)rnd.nextGaussian();
	}


	public static double d()
	{
		return rnd.nextGaussian() * rnd.nextInt(1<<24);
	}


	public static byte[] tb()
	{
		byte[] alpha = "abcdefghijklmnopqrstuvwxyzåäöABCDEFGHIJKLMNOPQRSTUVWXYZÅÖÖ".getBytes();
		byte[] buf = new byte[3 + rnd.nextInt(16)];
		for (int i = 0; i < buf.length; i++)
		{
			buf[i] = alpha[rnd.nextInt(alpha.length)];
		}
		return buf;
	}


	public static String t()
	{
		return new String(tb());
	}


	public static String compareObjects(Object msg0, Object msg1)
	{
		try
		{
			byte[] msg0buf;
			byte[] msg1buf;
			{
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				try (ObjectOutputStream oos = new ObjectOutputStream(baos))
				{
					oos.writeUnshared(msg0);
				}
				msg0buf = baos.toByteArray();
			}
			{
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				try (ObjectOutputStream oos = new ObjectOutputStream(baos))
				{
					oos.writeUnshared(msg1);
				}
				msg1buf = baos.toByteArray();
			}

			Log.hexDump(msg0buf);
			Log.out.println();
			Log.hexDump(msg1buf);

			return Arrays.equals(msg0buf, msg1buf) ? "Identical" : "Object references missmatch";
		}
		catch (IOException e)
		{
			return e.toString();
		}
	}
}

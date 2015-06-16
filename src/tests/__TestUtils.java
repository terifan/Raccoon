package tests;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Random;
import org.terifan.raccoon.util.Log;


public class __TestUtils
{
	public final static Random rnd = new Random(0);


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


	public static String t()
	{
		return new String(tb());
	}


	public static String t(int aMaxLength)
	{
		return new String(tb(aMaxLength));
	}


	public static byte[] tb()
	{
		return tb(16);
	}
	
	
	public static byte[] tb(int aMaxLength)
	{
		byte[] alpha = "abcdefghijklmnopqrstuvwxyzåäöABCDEFGHIJKLMNOPQRSTUVWXYZÅÖÖ".getBytes();
		byte[] buf = new byte[3 + rnd.nextInt(aMaxLength-3)];
		for (int i = 0; i < buf.length; i++)
		{
			buf[i] = alpha[rnd.nextInt(alpha.length)];
		}
		return buf;
	}



	public static byte[] createBuffer(long aSeed, int aLength)
	{
		Random r = new Random(aSeed);
		byte[] buf = new byte[aLength];
		r.nextBytes(buf);
		return buf;
	}


	public static void verifyBuffer(long aSeed, byte[] aBuffer)
	{
		Random r = new Random(aSeed);
		byte[] buf = new byte[aBuffer.length];
		r.nextBytes(buf);
		if (!Arrays.equals(aBuffer, buf))
		{
			throw new IllegalArgumentException("Data missmatch");
		}
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

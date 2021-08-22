package resources;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
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
		byte[] alpha = "abcdefghijklmnopqrstuvwxyzåäöABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ§ß©".getBytes();
		byte[] buf = new byte[3 + rnd.nextInt(aMaxLength-3)];
		for (int i = 0; i < buf.length; i++)
		{
			buf[i] = alpha[rnd.nextInt(alpha.length)];
		}
		return buf;
	}



	public static byte[] createRandomBuffer(long aSeed, int aLength)
	{
		Random r = new Random(aSeed);
		byte[] buf = new byte[aLength];
		r.nextBytes(buf);
		return buf;
	}


	public static boolean verifyRandomBuffer(long aSeed, byte[] aBuffer)
	{
		Random r = new Random(aSeed);
		byte[] buf = new byte[aBuffer.length];
		r.nextBytes(buf);
		return Arrays.equals(aBuffer, buf);
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


	public static byte[] readAll(InputStream aInputStream) throws IOException
	{
		try (InputStream in = aInputStream)
		{
			byte [] buffer = new byte[4096];
			ByteArrayOutputStream baos = new ByteArrayOutputStream();

			for (;;)
			{
				int len = in.read(buffer);

				if (len <= 0)
				{
					break;
				}

				baos.write(buffer, 0, len);
			}

			return baos.toByteArray();
		}
	}


	public static byte[] hexToBytes(String aValue)
	{
		if (aValue.length() % 2 != 0)
		{
			throw new IllegalArgumentException("aHexString must have an even length.");
		}

		byte[] output = new byte[aValue.length() / 2];

		for (int i = 0, j = 0; i < output.length; i++)
		{
			output[i] = (byte)((decodeChar(aValue.charAt(j++)) << 4) + decodeChar(aValue.charAt(j++)));
		}

		return output;
	}


	private static int decodeChar(char aByte)
	{
		if (aByte >= '0' && aByte <= '9')
		{
			return aByte - '0';
		}
		if (aByte >= 'a' && aByte <= 'z')
		{
			return 10 + aByte - 'a';
		}
		if (aByte >= 'A' && aByte <= 'Z')
		{
			return 10 + aByte - 'A';
		}
		throw new IllegalArgumentException();
	}
}

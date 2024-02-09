package org.terifan.raccoon;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Random;
import java.util.function.Supplier;
import javax.swing.JFrame;
import org.terifan.logging.Level;
import org.terifan.logging.Logger;
import org.terifan.raccoon.BTree;
import org.terifan.raccoon.ScanResult;
import org.terifan.raccoon.blockdevice.BlockAccessor;
import org.terifan.raccoon.blockdevice.managed.ManagedBlockDevice;
import org.terifan.raccoon.blockdevice.storage.MemoryBlockStorage;
import org.terifan.raccoon.blockdevice.secure.AccessCredentials;
import org.terifan.raccoon.blockdevice.secure.SecureBlockDevice;
import org.terifan.treegraph.HorizontalLayout;
import org.terifan.treegraph.TreeGraph;
import org.terifan.treegraph.util.VerticalImageFrame;
import org.terifan.raccoon.blockdevice.storage.BlockStorage;
import org.terifan.raccoon.document.Document;


public class _Tools
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


	public static Document doc()
	{
		return new Document().put("text", tb());
	}


	public static Document doc(int len)
	{
		return new Document().put("text", tb(len));
	}


	public static byte[] tb()
	{
		return tb(16);
	}


	public static byte[] tb(int aMaxLength)
	{
		byte[] alpha = "abcdefghijklmnopqrstuvwxyzåäöABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ§ß©".getBytes();
		byte[] buf = new byte[3 + rnd.nextInt(Math.max(1,aMaxLength - 3))];
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

			Logger.getLogger().hexDump(Level.ALL, msg0buf);
			Logger.getLogger().hexDump(Level.ALL, msg1buf);

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


	private static VerticalImageFrame mFrame;
	static TreeGraph mGraph;


	public static void showTree(BTree aTree)
	{
		if (mFrame == null)
		{
			mFrame = new VerticalImageFrame();
			mFrame.getFrame().setExtendedState(JFrame.MAXIMIZED_BOTH);
		}
		if (mGraph != null)
		{
			mFrame.remove(mGraph);
		}
		mGraph = new TreeGraph(new HorizontalLayout(), aTree.scan(new ScanResult()).getDescription());
		mFrame.add(mGraph);
	}


	public static BlockAccessor createMemoryStorage() throws IOException
	{
		return new BlockAccessor(new ManagedBlockDevice(new MemoryBlockStorage(512)), false);
	}


	public static BlockAccessor createSecureMemoryStorage() throws IOException
	{
		return new BlockAccessor(new ManagedBlockDevice(new SecureBlockDevice(new AccessCredentials("password"), new MemoryBlockStorage(512))), false);
	}


	public static BlockAccessor createStorage(Supplier<BlockStorage> aSupplier) throws IOException
	{
		BlockStorage device = aSupplier.get();
		if (device.size() > 0)
		{
			return new BlockAccessor(new ManagedBlockDevice(new SecureBlockDevice(new AccessCredentials("password"), device)), false);
		}
		return new BlockAccessor(new ManagedBlockDevice(device), false);
	}


	public static BlockAccessor createSecureStorage(Supplier<BlockStorage> aSupplier) throws IOException
	{
		BlockStorage device = aSupplier.get();
		if (device.size() > 0)
		{
			return new BlockAccessor(new ManagedBlockDevice(new SecureBlockDevice(new AccessCredentials("password"), device)), false);
		}
		return new BlockAccessor(new ManagedBlockDevice(new SecureBlockDevice(new AccessCredentials("password"), device)), false);
	}


	public static BlockAccessor createSecureStorage(BlockStorage aDevice) throws IOException
	{
		if (aDevice.size() > 0)
		{
			return new BlockAccessor(new ManagedBlockDevice(new SecureBlockDevice(new AccessCredentials("password"), aDevice)), false);
		}
		return new BlockAccessor(new ManagedBlockDevice(new SecureBlockDevice(new AccessCredentials("password"), aDevice)), false);
	}
}

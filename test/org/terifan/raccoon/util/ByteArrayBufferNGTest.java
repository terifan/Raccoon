package org.terifan.raccoon.util;

import java.io.EOFException;
import java.io.IOException;
import java.util.Random;
import static org.testng.Assert.*;
import org.testng.annotations.Test;


public class ByteArrayBufferNGTest
{
	public ByteArrayBufferNGTest()
	{
	}


	@Test
	public void testReadAndWriteVariableBuffer() throws IOException
	{
		Random rnd = new Random(1);
		byte[] buf = new byte[100];
		rnd.nextBytes(buf);

		rnd = new Random(1);

		ByteArrayBuffer out = new ByteArrayBuffer(50);
		for (int j = 0; j < 10; j++)
		{
			for (int i = rnd.nextInt(100); --i >= 0; )
			{
				out.write(rnd.nextInt() & 0xff);
			}
			out.writeInt32(rnd.nextInt());
			out.writeInt32(Integer.MIN_VALUE);
			out.writeInt32(Integer.MAX_VALUE);
			out.writeInt64(rnd.nextLong());
			out.writeInt64(Long.MIN_VALUE);
			out.writeInt64(Long.MAX_VALUE);
			out.writeVar32(rnd.nextInt());
			out.writeVar32(Integer.MIN_VALUE);
			out.writeVar32(Integer.MAX_VALUE);
			out.writeVar64(rnd.nextLong());
			out.writeVar64(Long.MIN_VALUE);
			out.writeVar64(Long.MAX_VALUE);
			out.write(buf);
			out.writeString("hello world");
		}

		ByteArrayBuffer in = new ByteArrayBuffer(out.trim().buffer());
		rnd = new Random(1);

		for (int j = 0; j < 10; j++)
		{
			for (int i = rnd.nextInt(100); --i >= 0; )
			{
				assertEquals(in.read(), rnd.nextInt() & 0xff);
			}
			assertEquals(in.readInt32(), rnd.nextInt());
			assertEquals(in.readInt32(), Integer.MIN_VALUE);
			assertEquals(in.readInt32(), Integer.MAX_VALUE);
			assertEquals(in.readInt64(), rnd.nextLong());
			assertEquals(in.readInt64(), Long.MIN_VALUE);
			assertEquals(in.readInt64(), Long.MAX_VALUE);
			assertEquals(in.readVar32(), rnd.nextInt());
			assertEquals(in.readVar32(), Integer.MIN_VALUE);
			assertEquals(in.readVar32(), Integer.MAX_VALUE);
			assertEquals(in.readVar64(), rnd.nextLong());
			assertEquals(in.readVar64(), Long.MIN_VALUE);
			assertEquals(in.readVar64(), Long.MAX_VALUE);
			assertEquals(in.read(new byte[100]), buf);
			assertEquals(in.readString(11), "hello world");
		}
	}


	@Test(expectedExceptions = EOFException.class)
	public void testWriteOverflow1() throws IOException
	{
		ByteArrayBuffer out = new ByteArrayBuffer(new byte[5]);
		out.writeInt64(0);
	}


	@Test(expectedExceptions = EOFException.class)
	public void testWriteOverflow2() throws IOException
	{
		ByteArrayBuffer out = new ByteArrayBuffer(new byte[5]);
		out.write(1);
		out.write(2);
		out.write(3);
		out.write(4);
		out.write(5);
		out.write(6);
	}
}

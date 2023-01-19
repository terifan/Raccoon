package org.terifan.raccoon.document;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.function.Supplier;


public class VarInputStream implements AutoCloseable, Iterable<Object>
{
	private final Checksum mChecksum;
	private InputStream mInputStream;


	public VarInputStream(InputStream aInputStream)
	{
		mInputStream = aInputStream;
		mChecksum = new Checksum();
	}


	private int readByte() throws IOException
	{
		int c = mInputStream.read();
		if (c == -1)
		{
			throw new EOFException();
		}
		mChecksum.update(c);
		return c;
	}


	private int readBytes(byte[] aBuffer) throws IOException
	{
		int len = mInputStream.read(aBuffer);
		mChecksum.update(aBuffer, 0, len);
		return len;
	}


	@Override
	public void close() throws IOException
	{
		if (mInputStream != null)
		{
			mInputStream.close();
			mInputStream = null;
		}
	}


	public Object readObject() throws IOException
	{
		Header header = readHeader();
		if (header.value != header.checksum)
		{
			throw new StreamChecksumException("Checksum error in input stream");
		}
		if (header.type == VarType.TERMINATOR)
		{
			return null;
		}
		return readValue(header.type);
	}


	@Override
	public Iterator<Object> iterator()
	{
		return new IteratorImpl<>(() -> {
			try
			{
				return readObject();
			}
			catch (IOException e)
			{
				return null;
			}
		});
	}


	private class IteratorImpl<T> implements Iterator<T>
	{
		T next;
		Supplier<T> supplier;


		IteratorImpl(Supplier<T> aSupplier)
		{
			supplier = aSupplier;
		}


		@Override
		public boolean hasNext()
		{
			if (mInputStream != null && next == null)
			{
				try
				{
					next = supplier.get();
					if (next == VarType.TERMINATOR)
					{
						next = null;
						close();
					}
				}
				catch (Exception | Error e)
				{
					try
					{
						close();
					}
					catch (Exception | Error ee)
					{
					}
				}
			}
			return next != null;
		}


		@Override
		public T next()
		{
			T tmp = next;
			next = null;
			return tmp;
		}
	}


	private Document readDocument() throws IOException
	{
		Document doc = new Document();

		for (;;)
		{
			Header header = readHeader();

			if (header.type == VarType.TERMINATOR)
			{
				if (header.value != header.checksum)
				{
					throw new StreamChecksumException("Checksum error in input stream");
				}
				break;
			}

			String key;
			if ((header.value & 1) == 1)
			{
				key = Integer.toString(header.value >>> 1);
			}
			else
			{
				key = readUTF(header.value >>> 1);
			}

			doc.putImpl(key, readValue(header.type));
		}

		return doc;
	}


	private Array readArray() throws IOException
	{
		Array array = new Array();

		for (;;)
		{
			Header header = readHeader();

			if (header.type == VarType.TERMINATOR)
			{
				if (header.value != header.checksum)
				{
					throw new StreamChecksumException("Checksum error in input stream");
				}
				break;
			}

			for (int i = 0; i < header.value; i++)
			{
				array.add(readValue(header.type));
			}
		}

		return array;
	}


	private Object readValue(VarType aType) throws IOException
	{
		switch (aType)
		{
			case DOCUMENT:
				return readDocument();
			case ARRAY:
				return readArray();
			default:
				return aType.decoder.decode(this);
		}
	}


	private Header readHeader() throws IOException
	{
		Header header = new Header();
		header.checksum = checksum();
		int[] params = readInterleaved();
		header.value = params[0];
		header.type = VarType.get(params[1]);
		return header;
	}


	private static class Header
	{
		int value;
		int checksum;
		VarType type;
	}


	String readString() throws IOException
	{
		return readUTF((int)readUnsignedVarint());
	}


	byte[] readBuffer() throws IOException
	{
		byte[] buffer = new byte[(int)readUnsignedVarint()];
		readBytes(buffer);
		return buffer;
	}


	long readVarint() throws IOException
	{
		for (long n = 0, result = 0; n < 64; n += 7)
		{
			int b = readByte();
			result += (long)(b & 127) << n;
			if (b < 128)
			{
				return (result >>> 1) ^ -(result & 1);
			}
		}

		throw new IllegalStateException("Variable int64 exceeds maximum length");
	}


	long readUnsignedVarint() throws IOException
	{
		for (long n = 0, result = 0; n < 64; n += 7)
		{
			int b = readByte();
			result += (long)(b & 127) << n;
			if (b < 128)
			{
				return result;
			}
		}

		throw new IllegalStateException("Variable int64 exceeds maximum length");
	}


	private String readUTF(int aLength) throws IOException
	{
		char[] output = new char[aLength];

		for (int i = 0; i < output.length; i++)
		{
			int c = readByte();

			if (c < 128) // 0xxxxxxx
			{
				output[i] = (char)c;
			}
			else if ((c & 0xE0) == 0xC0) // 110xxxxx
			{
				output[i] = (char)(((c & 0x1F) << 6) | (readByte() & 0x3F));
			}
			else if ((c & 0xF0) == 0xE0) // 1110xxxx
			{
				output[i] = (char)(((c & 0x0F) << 12) | ((readByte() & 0x3F) << 6) | (readByte() & 0x3F));
			}
			else
			{
				throw new IllegalStateException("This decoder only handles 16-bit characters: c = " + c);
			}
		}

		return new String(output);
	}


	private int[] readInterleaved() throws IOException
	{
		long p = readUnsignedVarint();
		return new int[]
		{
			reverseShift(p),
			reverseShift(p >>> 1)
		};
	}


	private static int reverseShift(long aWord)
	{
		aWord &= 0x5555555555555555L;

		aWord = (aWord | (aWord >>  1)) & 0x3333333333333333L;
		aWord = (aWord | (aWord >>  2)) & 0x0f0f0f0f0f0f0f0fL;
		aWord = (aWord | (aWord >>  4)) & 0x00ff00ff00ff00ffL;
		aWord = (aWord | (aWord >>  8)) & 0x0000ffff0000ffffL;
		aWord = (aWord | (aWord >> 16)) & 0x00000000ffffffffL;

		return (int)aWord;
	}


	int checksum()
	{
		return ((int)mChecksum.getValue()) & 0b1111;
	}
}
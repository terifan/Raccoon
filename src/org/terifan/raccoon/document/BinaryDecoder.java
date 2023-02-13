package org.terifan.raccoon.document;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.function.Supplier;


class BinaryDecoder implements AutoCloseable, Iterable<Object>
{
	private Checksum mChecksum;
	private InputStream mInputStream;


	public void unmarshal(InputStream aInputStream, Container aContainer) throws IOException
	{
		mInputStream = aInputStream;
		mChecksum = new Checksum();
		if (aContainer instanceof Document)
		{
			readDocument((Document)aContainer);
		}
		else
		{
			readArray((Array)aContainer);
		}
	}


	private int readByte() throws IOException
	{
		int c = mInputStream.read();
		if (c == -1)
		{
			throw new StreamException("Premature end of stream");
		}
		mChecksum.updateByte(c);
		return c;
	}


	public byte[] readBytes(byte[] aBuffer) throws IOException
	{
		mChecksum.update(aBuffer, 0, mInputStream.read(aBuffer));
		return aBuffer;
	}


	@Override
	public void close() throws IOException
	{
		mInputStream = null;
	}


	public Object readObject() throws IOException
	{
		Token token = readToken();
		if (token.value != token.checksum)
		{
			throw new StreamException("Checksum error in data stream");
		}
		if (token.type == BinaryType.TERMINATOR)
		{
			return null;
		}
		return readValue(token.type);
	}


	@Override
	public Iterator<Object> iterator()
	{
		return new IteratorImpl<>(() ->
		{
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
					if (next == BinaryType.TERMINATOR)
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


	private Document readDocument(Document aDocument) throws IOException
	{
		for (;;)
		{
			Token token = readToken();

			if (token.type == BinaryType.TERMINATOR)
			{
				if (token.value != token.checksum)
				{
					throw new StreamException("Checksum error in data stream");
				}
				break;
			}

			String key;
			if ((token.value & 1) == 1)
			{
				key = Integer.toString(token.value >>> 1);
			}
			else
			{
				key = readUTF(token.value >>> 1);
			}

			aDocument.putImpl(key, readValue(token.type));
		}

		return aDocument;
	}


	private Array readArray(Array aArray) throws IOException
	{
		for (;;)
		{
			Token token = readToken();

			if (token.type == BinaryType.TERMINATOR)
			{
				if (token.value != token.checksum)
				{
					throw new StreamException("Checksum error in data stream");
				}
				break;
			}

			for (int i = 0; i < token.value; i++)
			{
				aArray.add(readValue(token.type));
			}
		}

		return aArray;
	}


	private Object readValue(BinaryType aType) throws IOException
	{
		switch (aType)
		{
			case DOCUMENT:
				return readDocument(new Document());
			case ARRAY:
				return readArray(new Array());
			default:
				return aType.decoder.decode(this);
		}
	}


	private Token readToken() throws IOException
	{
		try
		{
			Token token = new Token();
			token.checksum = getChecksumValue();
			long params = readInterleaved();
			token.value = (int)(params >>> 32);
			token.type = BinaryType.values()[(int)params];
			return token;
		}
		catch (ArrayIndexOutOfBoundsException e)
		{
			throw new StreamException("Type parameter out of range");
		}
	}


	private static class Token
	{
		int value;
		int checksum;
		BinaryType type;
	}


	String readString() throws IOException
	{
		return readUTF((int)readUnsignedVarint());
	}


	byte[] readBuffer() throws IOException
	{
		return readBytes(new byte[(int)readUnsignedVarint()]);
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

		throw new StreamException("Variable int64 exceeds maximum length");
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

		throw new StreamException("Variable int64 exceeds maximum length");
	}


	private String readUTF(int aLength) throws IOException
	{
		if (aLength < 0)
		{
			throw new StreamException("Negative string length");
		}

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
				throw new StreamException("This decoder only handles 16-bit characters: c = " + c);
			}
		}

		return new String(output);
	}


	private long readInterleaved() throws IOException
	{
		long p = readUnsignedVarint();
		return (reverseShift(p) << 32) | reverseShift(p >>> 1);
	}


	private static long reverseShift(long aWord)
	{
		aWord &= 0x5555555555555555L;

		aWord = (aWord | (aWord >> 1)) & 0x3333333333333333L;
		aWord = (aWord | (aWord >> 2)) & 0x0f0f0f0f0f0f0f0fL;
		aWord = (aWord | (aWord >> 4)) & 0x00ff00ff00ff00ffL;
		aWord = (aWord | (aWord >> 8)) & 0x0000ffff0000ffffL;
		aWord = (aWord | (aWord >> 16)) & 0x00000000ffffffffL;

		return aWord;
	}


	private int getChecksumValue()
	{
		return mChecksum.getValue() & 0b1111;
	}
}

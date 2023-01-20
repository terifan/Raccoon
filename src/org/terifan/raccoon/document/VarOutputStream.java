package org.terifan.raccoon.document;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import static org.terifan.raccoon.document.VarType.identify;


class VarOutputStream implements AutoCloseable
{
	private final Checksum mChecksum;
	private OutputStream mOutputStream;


	public VarOutputStream(OutputStream aOutputStream)
	{
		mOutputStream = aOutputStream;
		mChecksum = new Checksum();
	}


	private void write(int aByte) throws IOException
	{
		mOutputStream.write(aByte & 0xFF);
		mChecksum.update(aByte);
	}


	private void write(byte[] aBuffer) throws IOException
	{
		mOutputStream.write(aBuffer);
		mChecksum.update(aBuffer, 0, aBuffer.length);
	}


	@Override
	public void close() throws IOException
	{
		if (mOutputStream != null)
		{
			writeHeader(checksum(), VarType.TERMINATOR);

			mOutputStream.close();
			mOutputStream = null;
		}
	}


	public void writeObject(Object aValue) throws IOException
	{
		VarType type = identify(aValue);
		writeHeader(checksum(), type);
		writeValue(type, aValue);
	}


	private void writeDocument(Document aDocument) throws IOException
	{
		for (Map.Entry<String, Object> entry : aDocument.entrySet())
		{
			Object value = entry.getValue();
			VarType type = identify(value);

			boolean numeric = entry.getKey().matches("[0-9]{1,}");
			if (numeric)
			{
				writeHeader((Integer.parseInt(entry.getKey()) << 1) + 1, type);
			}
			else
			{
				writeHeader((entry.getKey().length() << 1), type);
				writeUTF(entry.getKey());
			}

			writeValue(type, value);
		}

		writeHeader(checksum(), VarType.TERMINATOR);
	}


	private void writeArray(Array aArray) throws IOException
	{
		int elementCount = aArray.size();

		for (int offset = 0; offset < elementCount;)
		{
			VarType type = null;
			int runLen = 0;

			for (int i = offset; i < elementCount; i++, runLen++)
			{
				VarType nextType = identify(aArray.get(i));
				if (type != null && type != nextType)
				{
					break;
				}
				type = nextType;
			}

			writeHeader(runLen, type);

			while (--runLen >= 0)
			{
				writeValue(type, aArray.get(offset++));
			}
		}

		writeHeader(checksum(), VarType.TERMINATOR);
	}


	private void writeValue(VarType aType, Object aValue) throws IOException
	{
		switch (aType)
		{
			case DOCUMENT:
				writeDocument((Document)aValue);
				break;
			case ARRAY:
				writeArray((Array)aValue);
				break;
			default:
				aType.encoder.encode(this, aValue);
		}
	}


	private void writeHeader(int aValue, VarType aBinaryType) throws IOException
	{
		writeInterleaved(aValue, aBinaryType.code);
	}


	VarOutputStream writeString(String aValue) throws IOException
	{
		writeUnsignedVarint(aValue.length());
		writeUTF(aValue);
		return this;
	}


	VarOutputStream writeBuffer(byte[] aBuffer) throws IOException
	{
		writeUnsignedVarint(aBuffer.length);
		write(aBuffer);
		return this;
	}


	VarOutputStream writeVarint(long aValue) throws IOException
	{
		aValue = (aValue << 1) ^ (aValue >> 63);

		for (;;)
		{
			int b = (int)(aValue & 127);
			aValue >>>= 7;

			if (aValue == 0)
			{
				write(b);
				return this;
			}

			write(128 + b);
		}
	}


	VarOutputStream writeUnsignedVarint(long aValue) throws IOException
	{
		for (;;)
		{
			int b = (int)(aValue & 127);
			aValue >>>= 7;

			if (aValue == 0)
			{
				write(b);
				return this;
			}

			write(128 + b);
		}
	}


	private void writeUTF(String aInput) throws IOException
	{
		for (int i = 0, len = aInput.length(); i < len; i++)
		{
			char c = aInput.charAt(i);
		    if (c <= 0x007F)
		    {
				write(c & 0x7F);
		    }
		    else if (c <= 0x07FF)
		    {
				write(0xC0 | ((c >>  6) & 0x1F));
				write(0x80 | ((c      ) & 0x3F));
		    }
		    else
		    {
				write(0xE0 | ((c >> 12) & 0x0F));
				write(0x80 | ((c >>  6) & 0x3F));
				write(0x80 | ((c      ) & 0x3F));
		    }
		}
	}


	private void writeInterleaved(int aX, int aY) throws IOException
	{
		writeUnsignedVarint(shift(aX) | (shift(aY) << 1));
	}


	private static long shift(long aWord)
	{
		aWord &= 0xffffffffL;

		aWord = (aWord | (aWord << 16)) & 0x0000ffff0000ffffL;
		aWord = (aWord | (aWord <<  8)) & 0x00ff00ff00ff00ffL;
		aWord = (aWord | (aWord <<  4)) & 0x0f0f0f0f0f0f0f0fL;
		aWord = (aWord | (aWord <<  2)) & 0x3333333333333333L;
		aWord = (aWord | (aWord <<  1)) & 0x5555555555555555L;

		return aWord;
	}


	int checksum()
	{
		return ((int)mChecksum.getValue()) & 0b1111;
	}
}

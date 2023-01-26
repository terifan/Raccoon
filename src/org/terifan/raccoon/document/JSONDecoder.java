package org.terifan.raccoon.document;

import java.io.IOException;
import java.io.PushbackReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import org.terifan.raccoon.ObjectId;


class JSONDecoder
{
	private PushbackReader mReader;


	public Document unmarshal(Reader aReader, Document aDocument) throws IOException
	{
		mReader = new PushbackReader(aReader, 1);

		if (mReader.read() != '{')
		{
			throw new IllegalArgumentException();
		}

		return readDocument(aDocument);
	}


	public Array unmarshal(Reader aReader, Array aArray) throws IOException
	{
		mReader = new PushbackReader(aReader, 1);

		if (mReader.read() != '[')
		{
			throw new IllegalArgumentException();
		}

		return readArray(aArray);
	}


	private Document readDocument(Document aDocument) throws IOException
	{
		for (;;)
		{
			char c = readChar();

			if (c == '}')
			{
				break;
			}
			if (aDocument.size() > 0)
			{
				if (c != ',')
				{
					throw new IOException("Expected comma between elements");
				}

				c = readChar();
			}

			if (c == '}') // allow badly formatted json with unneccessary commas before ending brace
			{
				break;
			}
			if (c != '\"' && c != '\'')
			{
				throw new IOException("Expected starting quote character of key: " + c);
			}

			String key = readString(c);

			if (readChar() != ':')
			{
				throw new IOException("Expected colon sign after key: " + key);
			}

			aDocument.putImpl(key, readValue(readChar()));
		}

		return aDocument;
	}


	private Array readArray(Array aArray) throws IOException
	{
		for (;;)
		{
			char c = readChar();

			if (c == ']')
			{
				break;
			}
			if (c == ':')
			{
				throw new IOException("Found colon after element in array");
			}

			if (aArray.size() > 0)
			{
				if (c != ',')
				{
					throw new IOException("Expected comma between elements: found: " + c);
				}

				c = readChar();
			}

			aArray.add(readValue(c));
		}

		return aArray;
	}


	private Object readValue(char aChar) throws IOException
	{
		switch (aChar)
		{
			case '[':
				return readArray(new Array());
			case '{':
				return readDocument(new Document());
			case '\"':
			case '\'':
				return readString(aChar);
			default:
				mReader.unread(aChar);
				return readValue();
		}
	}


	private String readString(int aTerminator) throws IOException
	{
		StringBuilder sb = new StringBuilder();

		for (;;)
		{
			char c = readByte();

			if (c == aTerminator)
			{
				return sb.toString();
			}
			if (c == '\\')
			{
				c = readEscapeSequence();
			}

			sb.append(c);
		}
	}


	private Object readValue() throws IOException
	{
		StringBuilder sb = new StringBuilder();
		boolean terminator = false;

		for (;;)
		{
			char c = readByte();

			if (c == '}' || c == ']' || c == ',' || Character.isWhitespace(c))
			{
				terminator = c == '}' || c == ']';
				mReader.unread(c);
				break;
			}
			if (c == '\\')
			{
				c = readEscapeSequence();
			}

			sb.append(c);
		}

		String in = sb.toString().trim();

		if (terminator && "".equalsIgnoreCase(in))
		{
			throw new UnsupportedEncodingException();
		}

		if ("null".equalsIgnoreCase(in))
		{
			return null;
		}
		if ("true".equalsIgnoreCase(in))
		{
			return true;
		}
		if ("false".equalsIgnoreCase(in))
		{
			return false;
		}
		if (in.contains("."))
		{
			return Double.valueOf(in);
		}
		if (in.startsWith("0x"))
		{
			return Long.valueOf(in.substring(2), 16);
		}
		if (in.startsWith("ObjectId("))
		{
			return ObjectId.fromString(in.substring(9, in.length() - 1));
		}

		long v = Long.parseLong(in);
		if (v >= Byte.MIN_VALUE && v <= Byte.MAX_VALUE)
		{
			return (byte)v;
		}
		if (v >= Short.MIN_VALUE && v <= Short.MAX_VALUE)
		{
			return (short)v;
		}
		if (v >= Integer.MIN_VALUE && v <= Integer.MAX_VALUE)
		{
			return (int)v;
		}

		return v;
	}


	private char readEscapeSequence() throws IOException, NumberFormatException
	{
		char c = readByte();
		switch (c)
		{
			case '\"':
				return '\"';
			case '\\':
				return '\\';
			case 'n':
				return '\n';
			case 'r':
				return '\r';
			case 't':
				return '\t';
			case 'b':
				return '\b';
			case 'f':
				return '\f';
			case 'u':
				return (char)Integer.parseInt("" + readByte() + readByte() + readByte() + readByte(), 16);
			default:
				return c;
		}
	}


	private char readChar() throws IOException
	{
		for (;;)
		{
			char c = readByte();
			if (!Character.isWhitespace(c))
			{
				return c;
			}
		}
	}


	private char readByte() throws IOException
	{
		int c = mReader.read();
		if (c == -1)
		{
			throw new IOException("Unexpected end of stream.");
		}
		return (char)c;
	}
}

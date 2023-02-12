package org.terifan.raccoon.document;

import java.io.IOException;
import java.io.PushbackReader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Base64;
import java.util.UUID;
import org.terifan.raccoon.ObjectId;


class JSONDecoder
{
	private PushbackReader mReader;


	public <T extends Container> T unmarshal(String aJSON, T aContainer)
	{
		try
		{
			if (aContainer instanceof Document)
			{
				if (!aJSON.startsWith("{"))
				{
					aJSON = "{" + aJSON + "}";
				}

				mReader = new PushbackReader(new StringReader(aJSON), 1);
				mReader.read();

				return (T)readDocument((Document)aContainer);
			}
			else
			{
				if (!aJSON.startsWith("["))
				{
					aJSON = "[" + aJSON + "]";
				}

				mReader = new PushbackReader(new StringReader(aJSON), 1);
				mReader.read();

				return (T)readArray((Array)aContainer);
			}
		}
		catch (IOException e)
		{
			throw new IllegalArgumentException(e);
		}
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
//				throw new IOException("Expected starting quote character of key: " + c);
				mReader.unread(c);
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

			try
			{
				aArray.add(readValue(c));
			}
			catch (UnsupportedEncodingException e)
			{
				// ignore, an array ending with a delimiter will cause this intentionally
			}
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
		boolean unquoted = false;
		if (aTerminator != '\"' && aTerminator != '\'')
		{
			unquoted = true;
			aTerminator = 0;
		}

		StringBuilder sb = new StringBuilder();

		for (;;)
		{
			char c = readByte(unquoted);

			if (c == aTerminator || unquoted && (Character.isWhitespace(c) || c == ':' || c == ',' || c == '}' || c == ']'))
			{
				if (unquoted && c != 0)
				{
					mReader.unread(c);
				}
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
		if (in.startsWith("0x"))
		{
			return Long.valueOf(in.substring(2), 16);
		}
		if (in.startsWith("ObjectId("))
		{
			return ObjectId.fromString(in.substring(9, in.length() - 1));
		}
		if (in.startsWith("Base64("))
		{
			return Base64.getDecoder().decode(in.substring(7, in.length() - 1));
		}
		if (in.startsWith("UUID("))
		{
			return UUID.fromString(in.substring(5, in.length() - 1));
		}
		if (in.startsWith("Decimal("))
		{
			return new BigDecimal(in.substring(8, in.length() - 1));
		}
		if (in.startsWith("Date("))
		{
			return LocalDate.parse(in.substring(5, in.length() - 1));
		}
		if (in.startsWith("Time("))
		{
			return LocalTime.parse(in.substring(5, in.length() - 1));
		}
		if (in.startsWith("DateTime("))
		{
			if (in.contains("+"))
			{
				return OffsetDateTime.parse(in.substring(9, in.length() - 1));
			}
			return LocalDateTime.parse(in.substring(9, in.length() - 1));
		}
		if (in.contains("."))
		{
			return Double.valueOf(in);
		}

		try
		{
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
		catch (NumberFormatException e)
		{
			// ignore, faster to allow an exception than to regex the value before parsing
		}

		return in;
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


	private char readByte(boolean aUnquoted) throws IOException
	{
		int c = mReader.read();
		if (c == -1)
		{
			if (aUnquoted)
			{
				return 0;
			}
			throw new IOException("Unexpected end of stream.");
		}
		return (char)c;
	}
}

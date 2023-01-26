package org.terifan.raccoon.document;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Base64;
import java.util.Map.Entry;
import java.util.UUID;
import org.terifan.raccoon.ObjectId;


class JSONEncoder
{
	private JSONTextWriter mWriter;


	public void marshal(JSONTextWriter aPrinter, Container aContainer) throws IOException
	{
		mWriter = aPrinter;

		if (aContainer instanceof Document)
		{
			marshalDocument((Document)aContainer, true);
		}
		else
		{
			marshalArray((Array)aContainer);
		}
	}


	private void marshalDocument(Document aDocument) throws IOException
	{
		marshalDocument(aDocument, true);
	}


	private void marshalDocument(Document aDocument, boolean aNewLineOnClose) throws IOException
	{
		int size = aDocument.size();

		boolean hasDocument = aDocument.size() > 5;

		for (Object entry : aDocument.values())
		{
			if (entry instanceof Document)
			{
				hasDocument = true;
				break;
			}
		}

		if (!hasDocument && !mWriter.isFirst())
		{
			mWriter.println();
		}

		mWriter.println("{").indent(1);

		for (Entry<String, Object> entry : aDocument.entrySet())
		{
			mWriter.print("\"" + escapeString(entry.getKey()) + "\": ");

			marshal(entry.getValue());

			if (hasDocument && --size > 0)
			{
				mWriter.println(aNewLineOnClose ? "," : ", ", false);
			}
			else if (!hasDocument && --size > 0)
			{
				mWriter.print(", ", false);
			}
		}

		if (aNewLineOnClose)
		{
			mWriter.println().indent(-1).println("}");
		}
		else
		{
			mWriter.println().indent(-1).print("}");
		}
	}


	private void marshalArray(Array aArray) throws IOException
	{
		int size = aArray.size();

		if (size == 0)
		{
			mWriter.println("[]");
			return;
		}

		boolean special = aArray.get(0) instanceof Document;
		boolean first = special;
		boolean shortArray = !special && aArray.size() < 10;

		for (int i = 0; shortArray && i < aArray.size(); i++)
		{
			shortArray = !(aArray.get(i) instanceof Array) && !(aArray.get(i) instanceof Document) && !(aArray.get(i) instanceof String);
		}

		if (special)
		{
			mWriter.print("[").indent(aArray.size() > 1 ? 1 : 0);
		}
		else if (shortArray)
		{
			mWriter.print("[");
		}
		else
		{
			mWriter.println("[").indent(1);
		}

		for (Object value : aArray)
		{
			if (first)
			{
				marshalDocument((Document)value, false);

				if (--size > 0)
				{
					mWriter.println(", ");
				}
			}
			else
			{
				marshal(value);

				if (--size > 0)
				{
					mWriter.print(", ", false);
				}
			}

			first = false;
		}

		if (special)
		{
			mWriter.indent(aArray.size() > 1 ? -1 : 0).println("]");
		}
		else if (shortArray)
		{
			mWriter.println("]");
		}
		else
		{
			mWriter.println().indent(-1).println("]");
		}
	}


	private void marshal(Object aValue) throws IOException
	{
		if (aValue instanceof Document)
		{
			marshalDocument((Document)aValue);
		}
		else if (aValue instanceof Array)
		{
			marshalArray((Array)aValue);
		}
		else
		{
			marshalValue(aValue);
		}
	}


	private void marshalValue(Object aValue) throws IOException
	{
		if (aValue instanceof String
			|| aValue instanceof BigDecimal
//			|| aValue instanceof ObjectId
			|| aValue instanceof UUID
			|| aValue instanceof LocalDate
			|| aValue instanceof LocalTime
			|| aValue instanceof LocalDateTime
			|| aValue instanceof OffsetDateTime)
		{
			mWriter.print("\"" + escapeString(aValue.toString()) + "\"");
		}
		else if (aValue instanceof ObjectId)
		{
			mWriter.print("ObjectId(" + escapeString(aValue.toString()) + ")");
		}
		else if (aValue instanceof Number
			|| aValue instanceof Boolean)
		{
			mWriter.print(aValue);
		}
		else if (aValue == null)
		{
			mWriter.print("null");
		}
		else if (aValue instanceof byte[])
		{
			mWriter.print("\"" + marshalBinary((byte[])aValue) + "\"");
		}
		else
		{
			throw new IllegalArgumentException("Unsupported type: " + aValue.getClass());
		}
	}


	private String marshalBinary(byte[] aBuffer)
	{
		return Base64.getEncoder().encodeToString(aBuffer);
	}


	private String escapeString(String aString)
	{
		StringBuilder sb = new StringBuilder();
		for (int i = 0, len = aString.length(); i < len; i++)
		{
			char c = aString.charAt(i);
			switch (c)
			{
				case '\"':
					sb.append("\\\"");
					break;
				case '\\':
					sb.append("\\\\");
					break;
				case '\n':
					sb.append("\\n");
					break;
				case '\r':
					sb.append("\\r");
					break;
				case '\t':
					sb.append("\\t");
					break;
				case '\b':
					sb.append("\\b");
					break;
				case '\f':
					sb.append("\\f");
					break;
				default:
					if (c >= ' ')
					{
						sb.append(c);
					}
					else
					{
						sb.append(String.format("\\u%04X", (int)c));
					}
					break;
			}
		}
		return sb.toString();
	}
}

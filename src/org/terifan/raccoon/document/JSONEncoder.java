package org.terifan.raccoon.document;

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
	private StringBuilder mBuffer;
	private boolean mTyped;
	private boolean mCompact;
	private boolean mNewLine;
	private boolean mFirst;
	private int mIndent;


	public String marshal(KeyValueCollection aContainer, boolean aCompact, boolean aTyped)
	{
		mBuffer = new StringBuilder();
		mNewLine = false;
		mCompact = aCompact;
		mTyped = aTyped;
		mFirst = true;

		if (aContainer instanceof Document)
		{
			marshalDocument((Document)aContainer, true);
		}
		else
		{
			marshalArray((Array)aContainer);
		}

		return mBuffer.toString();
	}


	private void marshalDocument(Document aDocument)
	{
		marshalDocument(aDocument, true);
	}


	private void marshalDocument(Document aDocument, boolean aNewLineOnClose)
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

		if (!hasDocument && !isFirst())
		{
			println();
		}

		println("{");
		indent(1);

		for (Entry<String, Object> entry : aDocument.entrySet())
		{
			print("\"" + escapeString(entry.getKey()) + "\": ");

			marshal(entry.getValue());

			if (hasDocument && --size > 0)
			{
				println(aNewLineOnClose ? "," : ", ", false);
			}
			else if (!hasDocument && --size > 0)
			{
				print(", ", false);
			}
		}

		if (aNewLineOnClose)
		{
			println();
			indent(-1);
			println("}");
		}
		else
		{
			println();
			indent(-1);
			print("}");
		}
	}


	private void marshalArray(Array aArray)
	{
		int size = aArray.size();

		if (size == 0)
		{
			println("[]");
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
			print("[");
			indent(aArray.size() > 1 ? 1 : 0);
		}
		else if (shortArray)
		{
			print("[");
		}
		else
		{
			println("[");
			indent(1);
		}

		for (Object value : aArray)
		{
			if (first)
			{
				marshalDocument((Document)value, false);

				if (--size > 0)
				{
					println(", ");
				}
			}
			else
			{
				marshal(value);

				if (--size > 0)
				{
					print(", ", false);
				}
			}

			first = false;
		}

		if (special)
		{
			indent(aArray.size() > 1 ? -1 : 0);
			println("]");
		}
		else if (shortArray)
		{
			println("]");
		}
		else
		{
			println();
			indent(-1);
			println("]");
		}
	}


	private void marshal(Object aValue)
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


	private void marshalValue(Object aValue)
	{
		if (aValue instanceof String)
		{
			print("\"" + escapeString(aValue.toString()) + "\"");
		}
		else if (aValue == null)
		{
			print("null");
		}
		else if (
				aValue instanceof BigDecimal
			 || aValue instanceof ObjectId
			 || aValue instanceof UUID
			 || aValue instanceof LocalDate
			 || aValue instanceof LocalTime
			 || aValue instanceof LocalDateTime
			 || aValue instanceof OffsetDateTime)
		{
			if (!mTyped)
			{
				print("\"" + escapeString(aValue.toString()) + "\"");
			}
			else
			{
				if (aValue instanceof ObjectId)
				{
					print("ObjectId(" + escapeString(aValue.toString()) + ")");
				}
				else if (aValue instanceof byte[])
				{
					print("Base64(" + marshalBinary((byte[])aValue) + ")");
				}
				else if (aValue instanceof UUID)
				{
					print("UUID(" + aValue + ")");
				}
				else if (aValue instanceof BigDecimal)
				{
					print("Decimal(" + aValue + ")");
				}
				else if (aValue instanceof LocalDateTime)
				{
					print("DateTime(" + aValue + ")");
				}
				else if (aValue instanceof OffsetDateTime)
				{
					print("DateTime(" + aValue + ")");
				}
				else if (aValue instanceof LocalDate)
				{
					print("Date(" + aValue + ")");
				}
				else if (aValue instanceof LocalTime)
				{
					print("Time(" + aValue + ")");
				}
				else
				{
					throw new Error();
				}
			}
		}
		else if (aValue instanceof Number || aValue instanceof Boolean) // note: bigdecimal is number
		{
			print(aValue);
		}
		else if (aValue instanceof byte[])
		{
			print("\"" + marshalBinary((byte[])aValue) + "\"");
		}
		else
		{
			throw new IllegalArgumentException("Unsupported type: " + aValue.getClass());
		}
	}


	private String marshalBinary(byte[] aBuffer)
	{
		return Base64.getEncoder().encodeToString(aBuffer).replace("=", "");
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




	public void indent(int aDelta)
	{
		mIndent += aDelta;
	}


	public void print(Object aText)
	{
		print(aText, true);
	}


	public void print(Object aText, boolean aIndent)
	{
		String text = formatString(aText);

		if (mCompact && text.endsWith(" "))
		{
			text = stripTrailing(text);
			if (text.isEmpty())
			{
				return;
			}
		}

		if (aIndent)
		{
			printIndent();
		}

		mBuffer.append(text);
		mFirst = false;
	}


	public void println(Object aText)
	{
		println(aText, true);
	}


	public void println(Object aText, boolean aIndent)
	{
		String text = formatString(aText);

		if (mCompact && text.endsWith(" "))
		{
			text = stripTrailing(text);
			if (text.isEmpty())
			{
				return;
			}
		}

		if (aIndent)
		{
			printIndent();
		}

		mBuffer.append(text);
		mNewLine = true;
	}


	public void println()
	{
		mNewLine = true;
	}


	public boolean isFirst()
	{
		return mFirst;
	}


	private String formatString(Object aText)
	{
		if (aText == null)
		{
			return "null";
		}

		if (aText instanceof Double || aText instanceof Float)
		{
			String text = aText.toString().replace(" ", "");

			int i0 = text.indexOf(',');
			if (i0 != -1)
			{
				int i1 = text.indexOf('.');
				if (i1 != -1)
				{
					if (i0 < i1)
					{
						text = text.replace(",", ""); // handles: 10,000.7
					}
					else
					{
						text = text.replace(".", "").replace(',', '.'); // handles: 10.000,7
					}
				}
				else
				{
					text = text.replace(',', '.'); // handles: 10000.7
				}
			}

			if (text.endsWith(".0"))
			{
				text = text.substring(0, text.length() - 2);
			}

			return text;
		}

		return aText.toString();
	}


	private void printIndent()
	{
		if (mNewLine && !mCompact)
		{
			mBuffer.append("\n");
			for (int i = 0; i < mIndent; i++)
			{
				mBuffer.append("\t");
			}
			mNewLine = false;
		}
	}


	private String stripTrailing(String aText)
	{
		while (Character.isWhitespace(aText.charAt(aText.length() - 1)))
		{
			aText = aText.substring(0, aText.length() - 1);
		}
		return aText;
	}
}

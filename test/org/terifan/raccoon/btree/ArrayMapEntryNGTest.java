package org.terifan.raccoon.btree;

import org.terifan.raccoon.document.Document;
import org.testng.annotations.Test;
import static org.testng.Assert.*;


public class ArrayMapEntryNGTest
{
	@Test
	public void testPutGet()
	{
		ArrayMapEntry entry1 = new ArrayMapEntry(new ArrayMapKey("abc"), Document.of("def:1"), (byte)77);

		byte[] buf = new byte[10 + entry1.getMarshalledValueLength() + 10];

		entry1.marshallValue(buf, 10);

		ArrayMapEntry entry2 = new ArrayMapEntry();

		entry2.unmarshallValue(buf, 10, buf.length - 10 - 10);

		assertEquals(entry1.getType(), entry2.getType());
		assertEquals(entry1.getValue(), entry2.getValue());
	}
}

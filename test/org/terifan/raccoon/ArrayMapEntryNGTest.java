package org.terifan.raccoon;

import org.terifan.util.Debug;
import org.testng.annotations.Test;
import static org.testng.Assert.*;


public class ArrayMapEntryNGTest
{
	@Test
	public void testPutGet()
	{
		ArrayMapEntry entry1 = new ArrayMapEntry("abc".getBytes(), "def".getBytes(), (byte)77);

		byte[] buf = new byte[10 + entry1.getMarshalledValueLength() + 10];

		entry1.marshallValue(buf, 10);

		ArrayMapEntry entry2 = new ArrayMapEntry();

		entry2.unmarshallValue(buf, 10, buf.length - 10 - 10);

		assertEquals(entry1.getType(), entry2.getType());
		assertEquals(entry1.getValue(), entry2.getValue());
	}
}

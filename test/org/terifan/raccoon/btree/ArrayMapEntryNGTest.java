package org.terifan.raccoon.btree;

import org.terifan.raccoon.btree.ArrayMapEntry.Type;
import org.terifan.raccoon.document.Document;
import org.testng.annotations.Test;
import static org.testng.Assert.*;
import test_document._Log;


public class ArrayMapEntryNGTest
{
	@Test
	public void testPutGetKey()
	{
		ArrayMapEntry entry1 = new ArrayMapEntry().setKey("abc".getBytes(), Type.BYTEARRAY).setValue(Document.of("def:1").toByteArray(), Type.BYTEARRAY);

		byte[] buf = new byte[10 + entry1.getMarshalledKeyLength() + 10];

		entry1.getMarshalledKey(buf, 10);

		_Log.hexDump(buf);

		ArrayMapEntry entry2 = new ArrayMapEntry();

		System.out.println(buf.length);

		entry2.setMarshalledKey(buf, 10, buf.length - 10 - 10);

		assertEquals(entry2.getKey(), entry1.getKey());
	}


	@Test
	public void testPutGetValue()
	{
		ArrayMapEntry entry1 = new ArrayMapEntry().setKey("abc".getBytes(), Type.BYTEARRAY).setValue(Document.of("def:1").toByteArray(), Type.BYTEARRAY);

		byte[] buf = new byte[10 + entry1.getMarshalledValueLength() + 10];

		entry1.getMarshalledValue(buf, 10);

		_Log.hexDump(buf);

		ArrayMapEntry entry2 = new ArrayMapEntry();

		System.out.println(buf.length);

		entry2.setMarshalledValue(buf, 10, buf.length - 10 - 10);

		_Log.hexDump(entry1.getValue());
		_Log.hexDump(entry2.getValue());

		assertEquals(entry2.getValue(), entry1.getValue());
	}
}

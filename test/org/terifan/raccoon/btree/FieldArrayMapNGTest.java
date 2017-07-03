package org.terifan.raccoon.btree;

import java.io.IOException;
import java.util.ArrayList;
import org.terifan.raccoon.core.RecordEntry;
import org.terifan.raccoon.serialization.FieldDescriptor;
import org.terifan.raccoon.serialization.FieldWriter;
import org.terifan.raccoon.serialization.ValueType;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;
import static org.testng.Assert.*;
import org.testng.annotations.Test;


public class FieldArrayMapNGTest
{
	@Test
	public void testIndexOf() throws IOException
	{
//		ArrayList<FieldDescriptor> fields = new ArrayList<>();
//		fields.add(new FieldDescriptor(0, 1, 0, ValueType.INT, false, false, true, "test", "test", null));
//
//		ByteArrayBuffer key = new ByteArrayBuffer(512);
//		FieldWriter.writeField(fields.get(0), 13, key);
//		key.trim();
//
//		byte[] value = "abc".getBytes();
//
//		byte[] buffer = new byte[512];
//		FieldArrayMap map = new FieldArrayMap(fields, buffer);
//
//		RecordEntry entry = new RecordEntry(key.array(), value, (byte)0);
//
//		map.put(entry);
//
//		Log.hexDump(buffer);
//		
//		assertTrue(true);
	}
}

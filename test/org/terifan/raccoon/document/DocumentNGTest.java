package org.terifan.raccoon.document;

import org.terifan.raccoon.util.Log;
import static org.testng.Assert.*;
import org.testng.annotations.Test;


public class DocumentNGTest
{
	@Test
	public void testSomeMethod()
	{
		Document source = new Document().put("_id", 1).put("text", "hello").put("array", Array.of(1, 2, 3));

		byte[] data = source.marshal();

		Log.hexDump(data);

		Document unmarshaled = Document.unmarshal(data);

		assertEquals(unmarshaled, source);
	}
}

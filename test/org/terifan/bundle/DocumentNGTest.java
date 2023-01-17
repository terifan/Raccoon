package org.terifan.bundle;

import org.terifan.raccoon.document.Array;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.util.Log;
import static org.testng.Assert.*;
import org.testng.annotations.Test;


public class DocumentNGTest
{
	@Test
	public void testSomeMethod()
	{
		Document source = new Document().put("_id", 1).put("text", "hello world").put("values", Array.of(1, 2, 3));

		byte[] data = source.marshal();

//		Log.hexDump(data);

		Document unmarshaled = Document.unmarshal(data);

		assertEquals(unmarshaled, source);
	}
}

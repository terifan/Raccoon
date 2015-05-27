package org.terifan.raccoon.serialization;

import org.terifan.raccoon.util.Log;
import static org.testng.Assert.*;
import org.testng.annotations.Test;


public class MarshallerNGTest
{
	public MarshallerNGTest()
	{
	}


	@Test
	public void testSomeMethod() throws Exception
	{
		try
		{
			MsgBasicTypes in = new MsgBasicTypes();
			in.random();

			byte[] buffer = new Marshaller(MsgBasicTypes.class).marshal(in, FieldCategory.VALUE);

			Log.hexDump(buffer);

			MsgBasicTypes out = new MsgBasicTypes();

			new Marshaller(MsgBasicTypes.class).unmarshal(out, buffer);

			assertEquals(out.mDate, in.mDate);
			assertEquals(out.mDateList, in.mDateList);
			assertEquals(out.mDateMap, in.mDateMap);
			assertEquals(out.mString, in.mString);
			assertEquals(out.mStringList, in.mStringList);
			assertEquals(out.mStringMap, in.mStringMap);
			assertEquals(out.mDouble, in.mDouble);
			assertEquals(out.mDoubleList, in.mDoubleList);
			assertEquals(out.mDoubleMap, in.mDoubleMap);
		}
		catch (Exception e)
		{
			e.printStackTrace(Log.out);

			fail();
		}
	}
}

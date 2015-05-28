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
			Log.LEVEL = 10;

			_BigObject in = new _BigObject();
			in.random();

			Marshaller marshaller = new Marshaller(_BigObject.class);
			byte[] types = marshaller.getTypeDeclarations();
			byte[] buffer = marshaller.marshal(in, FieldCategory.VALUE);

			Log.out.println("---------");
			Log.hexDump(types);
			Log.out.println("---------");
			Log.hexDump(buffer);
			Log.out.println("---------");

			_BigObject out = new _BigObject();

			new Marshaller(types).unmarshal(buffer, out);

//			assertEquals(out.mString, in.mString);
//			assertEquals(out.mDate, in.mDate);
//			assertEquals(out.mDateList, in.mDateList);
//			assertEquals(out.mDateMap, in.mDateMap);
//			assertEquals(out.mString, in.mString);
//			assertEquals(out.mStringList, in.mStringList);
//			assertEquals(out.mStringMap, in.mStringMap);
//			assertEquals(out.mDouble, in.mDouble);
//			assertEquals(out.mDoubleList, in.mDoubleList);
//			assertEquals(out.mDoubleMap, in.mDoubleMap);
		}
		catch (Exception e)
		{
			e.printStackTrace(Log.out);

			fail();
		}
	}
}

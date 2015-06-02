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
		_BigObject in = new _BigObject();
		in.random();

		Marshaller marshaller = new Marshaller(_BigObject.class);
		TypeDeclarations types = marshaller.getTypeDeclarations();
		byte[] buffer = marshaller.marshal(in, FieldCategory.VALUE);

		_BigObject out = new _BigObject();

		new Marshaller(types).unmarshal(buffer, out, FieldCategory.VALUE);

		assertEquals(out.mString, in.mString);
		assertEquals(out.mDate, in.mDate);
		assertEquals(out.mDateList, in.mDateList);
		assertEquals(out.mDateMap, in.mDateMap);
		assertEquals(out.mString, in.mString);
		assertEquals(out.mStringList, in.mStringList);
		assertEquals(out.mStringMap, in.mStringMap);
		assertEquals(out.mDouble, in.mDouble);
		assertEquals(out.mDoubleList, in.mDoubleList);
		assertEquals(out.mDoubleMap, in.mDoubleMap);
		assertEquals(out.mStringMapArray.values().iterator().next(), in.mStringMapArray.values().iterator().next());
	}
}

package org.terifan.raccoon.serialization;

import org.terifan.raccoon.serialization.old.FieldCategory;
import org.terifan.raccoon.serialization.old.TypeDeclarations;
import org.terifan.raccoon.serialization.old.Marshaller;
import static org.testng.Assert.*;
import org.testng.annotations.Test;
import tests._BigObject1K;


public class MarshallerNGTest
{
	@Test
	public void testSomeMethod() throws Exception
	{
		_BigObject1K in = new _BigObject1K();
		in.random();

		Marshaller marshaller = new Marshaller(_BigObject1K.class);
		TypeDeclarations types = marshaller.getTypeDeclarations();
		byte[] buffer = marshaller.marshal(in, FieldCategory.VALUES);

		_BigObject1K out = new _BigObject1K();

		new Marshaller(types).unmarshal(buffer, out, FieldCategory.VALUES);

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

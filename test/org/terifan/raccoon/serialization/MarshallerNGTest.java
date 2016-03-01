package org.terifan.raccoon.serialization;

import java.util.Collection;
import static org.testng.Assert.*;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import tests._BigObject2K1D;


public class MarshallerNGTest
{
	@Test(dataProvider = "x")
	public void testSomeMethod(Collection<FieldCategory> aFieldCategories) throws Exception
	{
		_BigObject2K1D in = new _BigObject2K1D().random();

		EntityDescriptor tableDescriptor = new EntityDescriptor(_BigObject2K1D.class);

		Marshaller marshaller = new Marshaller(tableDescriptor);
		byte[] buffer = marshaller.marshal(in, aFieldCategories);

		_BigObject2K1D out = new _BigObject2K1D();

		new Marshaller(tableDescriptor).unmarshal(buffer, out, aFieldCategories);

		if (aFieldCategories.contains(FieldCategory.KEY))
		{
			assertEquals(out._key1, in._key1);
			assertEquals(out._key2, in._key2);
		}
		else
		{
			assertEquals(out._key1, null);
			assertEquals(out._key2, null);
		}

		if (aFieldCategories.contains(FieldCategory.DISCRIMINATOR))
		{
			assertEquals(out._discriminator, in._discriminator);
		}
		else
		{
			assertEquals(out._discriminator, null);
		}

		if (aFieldCategories.contains(FieldCategory.VALUE))
		{
			assertEquals(out.mString, in.mString);
			assertEquals(out.mDate, in.mDate);
			assertEquals(out.mChars, in.mChars);
			assertEquals(out.mDouble, in.mDouble);
			assertEquals(out.mDoubles2B, in.mDoubles2B);
			assertEquals(out.mFloats2, in.mFloats2);
			assertEquals(out.mBooleans2, in.mBooleans2);
		}
	}
	
	@DataProvider
	private Object[][] x()
	{
		return new Object[][]{
			{FieldCategoryFilter.ALL}, 
			{FieldCategoryFilter.KEYS},
			{FieldCategoryFilter.DISCRIMINATORS},
			{FieldCategoryFilter.VALUES},
			{FieldCategoryFilter.DISCRIMINATORS_VALUES}
		};
	}
}

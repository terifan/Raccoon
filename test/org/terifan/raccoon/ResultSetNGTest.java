package org.terifan.raccoon;

import org.testng.annotations.Test;


public class ResultSetNGTest
{
//	@Test
//	public void unmarshallResultSet() throws Exception
//	{
//		_BigObject2K1D in = new _BigObject2K1D().random();
//
//		EntityDescriptor entityDescriptor = new EntityDescriptor(_BigObject2K1D.class, mCategorizer);
//
//		ByteArrayBuffer buffer = new ByteArrayBuffer(16);
//
//		Marshaller marshaller = new Marshaller(entityDescriptor);
//		marshaller.marshal(buffer, in, 1 + 2 + 4);
//
//		buffer.position(0);
//
//		ResultSet resultSet = new ResultSet(entityDescriptor).unmarshal(buffer, 1 + 2 + 4);
//
//		assertEquals(resultSet.get("mFloatB"), in.mFloatB);
//		assertEquals(resultSet.get("mStrings2"), in.mStrings2);
//		assertEquals(resultSet.get("mChars"), in.mChars);
//	}
//
//
//	static FieldTypeCategorizer mCategorizer = aField ->
//	{
//		if (aField.getAnnotation(Discriminator.class) != null)
//		{
//			return 2;
//		}
//		else if (aField.getAnnotation(Key.class) != null)
//		{
//			return 1;
//		}
//		return 4;
//	};
}

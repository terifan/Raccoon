package org.terifan.raccoon;

import org.terifan.raccoon.serialization.EntityDescriptor;
import org.terifan.raccoon.serialization.FieldTypeCategorizer;
import org.terifan.raccoon.serialization.Marshaller;
import org.terifan.raccoon.util.ByteArrayBuffer;
import static org.testng.Assert.*;
import org.testng.annotations.Test;
import resources.entities._BigObject2K1D;


public class ResultSetNGTest
{
	@Test
	public void unmarshallResultSet() throws Exception
	{
		_BigObject2K1D in = new _BigObject2K1D().random();

		EntityDescriptor entityDescriptor = new EntityDescriptor(_BigObject2K1D.class, mCategorizer);

		ByteArrayBuffer buffer = new ByteArrayBuffer(16);

		Marshaller marshaller = new Marshaller(entityDescriptor);
		marshaller.marshal(buffer, in, 2 + 4);

		buffer.position(0);
		ResultSet resultSet = marshaller.unmarshal(buffer, new ResultSet(), 2 + 4);

		assertEquals(resultSet.get("mFloatB"), in.mFloatB);
		assertEquals(resultSet.get("mStrings2"), in.mStrings2);
		assertEquals(resultSet.get("mChars"), in.mChars);
	}


	static FieldTypeCategorizer mCategorizer = aField ->
	{
		if (aField.getAnnotation(Discriminator.class) != null)
		{
			return 2;
		}
		else if (aField.getAnnotation(Key.class) != null)
		{
			return 1;
		}
		return 4;
	};
}

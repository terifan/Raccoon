package org.terifan.raccoon.serialization;

import org.terifan.raccoon.util.ByteArrayBuffer;
import static org.testng.Assert.*;
import org.testng.annotations.Test;
import tests._BigObject2K1D;


public class MarshallerNGTest
{
	@Test
	public void marshallKeys() throws Exception
	{
		_BigObject2K1D in = new _BigObject2K1D().random();

		EntityDescriptor tableDescriptor = EntityDescriptor.getInstance(_BigObject2K1D.class);

		ByteArrayBuffer buffer = new ByteArrayBuffer(16);

		Marshaller marshaller = Marshaller.getInstance(tableDescriptor);
		marshaller.marshalKeys(buffer, in);

		_BigObject2K1D out = new _BigObject2K1D();

		buffer.position(0);

		Marshaller.getInstance(tableDescriptor).unmarshalKeys(buffer, out);

		assertEquals(out._key1, in._key1);
		assertEquals(out._key2, in._key2);
		assertEquals(out._discriminator, null);
	}


	@Test
	public void marshallValues() throws Exception
	{
		_BigObject2K1D in = new _BigObject2K1D().random();

		EntityDescriptor tableDescriptor = EntityDescriptor.getInstance(_BigObject2K1D.class);

		ByteArrayBuffer buffer = new ByteArrayBuffer(16);

		Marshaller marshaller = Marshaller.getInstance(tableDescriptor);
		marshaller.marshalValues(buffer, in);

		_BigObject2K1D out = new _BigObject2K1D();

		buffer.position(0);

		Marshaller.getInstance(tableDescriptor).unmarshalValues(buffer, out);

		assertEquals(out._discriminator, in._discriminator);
		assertEquals(out.mString, in.mString);
		assertEquals(out.mDate, in.mDate);
		assertEquals(out.mChars, in.mChars);
		assertEquals(out.mDouble, in.mDouble);
		assertEquals(out.mDoubles2B, in.mDoubles2B);
		assertEquals(out.mFloats2, in.mFloats2);
		assertEquals(out.mBooleans2, in.mBooleans2);
	}
}

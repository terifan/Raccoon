package org.terifan.raccoon.serialization;

import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.ResultSet;
import static org.testng.Assert.*;
import org.testng.annotations.Test;
import resources.entities._BigObject2K1D;


public class MarshallerNGTest
{
	@Test
	public void marshallKeys() throws Exception
	{
		_BigObject2K1D in = new _BigObject2K1D().random();

		EntityDescriptor entityDescriptor = EntityDescriptorFactory.getInstance(_BigObject2K1D.class);

		ByteArrayBuffer buffer = new ByteArrayBuffer(16);

		Marshaller marshaller = MarshallerFactory.getInstance(entityDescriptor);
		marshaller.marshal(buffer, in, 1);

		_BigObject2K1D out = new _BigObject2K1D();

		buffer.position(0);

		MarshallerFactory.getInstance(entityDescriptor).unmarshal(buffer, out, 1);

		assertEquals(out._key1, in._key1);
		assertEquals(out._key2, in._key2);
		assertEquals(out._discriminator, null);
	}


	@Test
	public void marshallDiscriminatorAndValues() throws Exception
	{
		_BigObject2K1D in = new _BigObject2K1D().random();

		EntityDescriptor entityDescriptor = EntityDescriptorFactory.getInstance(_BigObject2K1D.class);

		System.out.println(entityDescriptor);

		ByteArrayBuffer buffer = new ByteArrayBuffer(16);

		Marshaller marshaller = MarshallerFactory.getInstance(entityDescriptor);
		marshaller.marshal(buffer, in, 2 + 4);

		_BigObject2K1D out = new _BigObject2K1D();

		buffer.position(0);
		marshaller.unmarshal(buffer, out, 2 + 4);

		assertEquals(out._discriminator, in._discriminator);
		assertEquals(out.mString, in.mString);
		assertEquals(out.mDate, in.mDate);
		assertEquals(out.mChars, in.mChars);
		assertEquals(out.mDouble, in.mDouble);
		assertEquals(out.mDoubles2B, in.mDoubles2B);
		assertEquals(out.mFloats2, in.mFloats2);
		assertEquals(out.mBooleans2, in.mBooleans2);
	}


	@Test
	public void unmarshallResultSet() throws Exception
	{
		_BigObject2K1D in = new _BigObject2K1D().random();

		EntityDescriptor entityDescriptor = EntityDescriptorFactory.getInstance(_BigObject2K1D.class);

		System.out.println(entityDescriptor);

		ByteArrayBuffer buffer = new ByteArrayBuffer(16);

		Marshaller marshaller = MarshallerFactory.getInstance(entityDescriptor);
		marshaller.marshal(buffer, in, 2 + 4);

		buffer.position(0);
		ResultSet resultSet = marshaller.unmarshal(buffer, new ResultSet(), 2 + 4);

		System.out.println(resultSet);
	}
}

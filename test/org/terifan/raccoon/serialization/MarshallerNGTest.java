package org.terifan.raccoon.serialization;

import org.terifan.raccoon.annotations.Discriminator;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;
import org.terifan.raccoon.ResultSet;
import static org.testng.Assert.*;
import org.testng.annotations.Test;
import resources.entities._BigObject2K1D;
import resources.entities._Fruit1K1D;
import org.terifan.raccoon.annotations.Id;


public class MarshallerNGTest
{
	@Test
	public void marshallKeys() throws Exception
	{
		_BigObject2K1D in = new _BigObject2K1D().random();

		EntityDescriptor entityDescriptor = new EntityDescriptor(_BigObject2K1D.class, mCategorizer);

		ByteArrayBuffer buffer = ByteArrayBuffer.alloc(16);

		Marshaller marshaller = new Marshaller(entityDescriptor);
		marshaller.marshal(buffer, in, 1);

		_BigObject2K1D out = new _BigObject2K1D();

		buffer.position(0);

		new Marshaller(entityDescriptor).unmarshal(buffer, out, 1);

		assertEquals(out._key1, in._key1);
		assertEquals(out._key2, in._key2);
		assertEquals(out._discriminator, null);
	}


	@Test
	public void marshallDiscriminatorAndValues() throws Exception
	{
		_BigObject2K1D in = new _BigObject2K1D().random();

		EntityDescriptor entityDescriptor = new EntityDescriptor(_BigObject2K1D.class, mCategorizer);

		ByteArrayBuffer buffer = ByteArrayBuffer.alloc(16);

		Marshaller marshaller = new Marshaller(entityDescriptor);
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


//	@Test
//	public void marshall() throws Exception
//	{
//		Object in = new _Fruit1K1D("red", "test", 64.64);
//
//		EntityDescriptor entityDescriptor = new EntityDescriptor(in.getClass(), mCategorizer);
//
//		ByteArrayBuffer buffer = ByteArrayBuffer.alloc(16);
//
//		Marshaller marshaller = new Marshaller(entityDescriptor);
//		marshaller.marshal(buffer, in, 1 + 2 + 4);
//
//		Log.hexDump(buffer.trim().array());
//	}


	static FieldTypeCategorizer mCategorizer = aField ->
	{
		if (aField.getAnnotation(Discriminator.class) != null)
		{
			return 2;
		}
		else if (aField.getAnnotation(Id.class) != null)
		{
			return 1;
		}
		return 4;
	};
}

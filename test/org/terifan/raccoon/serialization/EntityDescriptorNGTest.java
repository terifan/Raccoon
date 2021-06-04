package org.terifan.raccoon.serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import static org.testng.Assert.*;
import org.testng.annotations.Test;
import resources.entities._BigObject2K1D;
import static org.terifan.raccoon.serialization.MarshallerNGTest.mCategorizer;


public class EntityDescriptorNGTest
{
	@Test
	public void testSerialization() throws IOException, ClassNotFoundException
	{
		EntityDescriptor out = new EntityDescriptor("obj", _BigObject2K1D.class, mCategorizer);

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try (ObjectOutputStream oos = new ObjectOutputStream(baos))
		{
			out.writeExternal(oos);
		}

		EntityDescriptor in = new EntityDescriptor();
		in.readExternal(new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray())));

		assertEquals(out.toString(), in.toString());
	}


	@Test
	public void testToString() throws IOException, ClassNotFoundException
	{
		EntityDescriptor out = new EntityDescriptor("obj", _BigObject2K1D.class, mCategorizer);

		assertTrue(out.toString().length() > 0);
	}


	@Test
	public void testEquals() throws IOException, ClassNotFoundException
	{
		EntityDescriptor a = new EntityDescriptor("obj", _BigObject2K1D.class, mCategorizer);
		EntityDescriptor b = new EntityDescriptor("obj", _BigObject2K1D.class, mCategorizer);
		boolean eq = a.equals(b);

		assertTrue(eq);
		assertEquals(a.getFields(), b.getFields());
	}


	@Test
	public void testEquals2() throws IOException, ClassNotFoundException
	{
		EntityDescriptor a = new EntityDescriptor("obj", _BigObject2K1D.class, mCategorizer);
		EntityDescriptor b = null;
		boolean eq = a.equals(b);

		assertFalse(eq);
	}


	@Test
	public void testGetName() throws IOException, ClassNotFoundException
	{
		EntityDescriptor a = new EntityDescriptor("obj", _BigObject2K1D.class, mCategorizer);

		assertEquals(a.getTypeName(), _BigObject2K1D.class.getName());
	}


	@Test
	public void testHashCode() throws IOException, ClassNotFoundException
	{
		EntityDescriptor a = new EntityDescriptor("obj", _BigObject2K1D.class, mCategorizer);
		EntityDescriptor b = new EntityDescriptor("obj", _BigObject2K1D.class, mCategorizer);

		assertEquals(a.hashCode(), b.hashCode());
		assertEquals(a.getFields()[0].hashCode(), b.getFields()[0].hashCode());
	}
}

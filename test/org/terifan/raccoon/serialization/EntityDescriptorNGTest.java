package org.terifan.raccoon.serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import static org.testng.Assert.*;
import org.testng.annotations.Test;
import tests._BigObject2K1D;


public class EntityDescriptorNGTest
{
	@Test
	public void testSerialization() throws IOException, ClassNotFoundException
	{
		EntityDescriptor out = EntityDescriptor.getInstance(_BigObject2K1D.class);

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
		EntityDescriptor out = EntityDescriptor.getInstance(_BigObject2K1D.class);

		assertTrue(out.toString().length() > 0);
	}


	@Test
	public void testEquals() throws IOException, ClassNotFoundException
	{
		EntityDescriptor a = EntityDescriptor.getInstance(_BigObject2K1D.class);
		EntityDescriptor b = EntityDescriptor.getInstance(_BigObject2K1D.class);

		assertEquals(a, b);
		assertEquals(a.getKeyFields(), b.getKeyFields());
		assertEquals(a.getDiscriminatorFields(), b.getDiscriminatorFields());
		assertEquals(a.getValueFields(), b.getValueFields());
	}


	@Test
	public void testHashCode() throws IOException, ClassNotFoundException
	{
		EntityDescriptor a = EntityDescriptor.getInstance(_BigObject2K1D.class);
		EntityDescriptor b = EntityDescriptor.getInstance(_BigObject2K1D.class);

		assertEquals(a.hashCode(), b.hashCode());
		assertEquals(a.getValueFields()[0].hashCode(), b.getValueFields()[0].hashCode());
	}
}

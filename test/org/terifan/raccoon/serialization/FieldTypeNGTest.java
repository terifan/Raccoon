package org.terifan.raccoon.serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import static org.testng.Assert.*;
import org.testng.annotations.Test;
import tests._BigObject2K1D;


public class FieldTypeNGTest
{
	@Test
	public void testSomeMethod() throws IOException, ClassNotFoundException
	{
//		EntityDescriptor out = new EntityDescriptor(_BigObject2K1D.class);
//
//		ByteArrayOutputStream baos = new ByteArrayOutputStream();
//		try (ObjectOutputStream oos = new ObjectOutputStream(baos))
//		{
//			out.writeExternal(oos);
//		}
//		
//		EntityDescriptor in = new EntityDescriptor();
//		in.readExternal(new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray())));
//
//		assertEquals(out.toString(), in.toString());
	}
}

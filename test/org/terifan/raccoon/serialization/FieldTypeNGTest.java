package org.terifan.raccoon.serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import static org.testng.Assert.*;
import org.testng.annotations.Test;
import tests._BigObject1K;


public class FieldTypeNGTest
{
	@Test
	public void testSomeMethod() throws IOException, ClassNotFoundException
	{
		Marshaller marshaller = new Marshaller(_BigObject1K.class);
		TypeDeclarations out = marshaller.getTypeDeclarations();
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try (ObjectOutputStream oos = new ObjectOutputStream(baos))
		{
			out.writeExternal(oos);
		}
		
		TypeDeclarations in = new TypeDeclarations();
		in.readExternal(new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray())));

		assertEquals(out.toString(), in.toString());
	}
}

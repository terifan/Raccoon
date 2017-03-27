package org.terifan.raccoon;

import static org.testng.Assert.*;
import org.testng.annotations.Test;
import resources.entities._Fruit1K1D;


public class TableMetadataNGTest
{
	public TableMetadataNGTest()
	{
	}


	@Test
	public void testEquals()
	{
		TableMetadata metadata1 = new TableMetadata(_Fruit1K1D.class, new DiscriminatorType(new _Fruit1K1D("red")));

		TableMetadata metadata2 = new TableMetadata(_Fruit1K1D.class, new DiscriminatorType(new _Fruit1K1D("red")));

		assertEquals(metadata1, metadata2);
	}


	@Test
	public void testNotEquals()
	{
		TableMetadata metadata1 = new TableMetadata(_Fruit1K1D.class, new DiscriminatorType(new _Fruit1K1D("red")));
		assertNotEquals(metadata1, null);
	}


	@Test
	public void testToString()
	{
		TableMetadata metadata = new TableMetadata(_Fruit1K1D.class, new DiscriminatorType(new _Fruit1K1D("red")));

		assertEquals("resources.entities._Fruit1K1D[_color=red]", metadata.toString());
	}


	@Test
	public void testDiscriminatorDescription()
	{
		TableMetadata metadata = new TableMetadata(_Fruit1K1D.class, new DiscriminatorType(new _Fruit1K1D("red")));

		assertEquals("_color=red", metadata.getDiscriminatorDescription());
	}
}

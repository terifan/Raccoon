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
		EntityDescriptor out = EntityDescriptorRegistry.getInstance(_BigObject2K1D.class);

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
		EntityDescriptor out = EntityDescriptorRegistry.getInstance(_BigObject2K1D.class);

		assertTrue(out.toString().length() > 0);
	}


	@Test
	public void testEquals() throws IOException, ClassNotFoundException
	{
		EntityDescriptor a = EntityDescriptorRegistry.getInstance(_BigObject2K1D.class);
		EntityDescriptor b = EntityDescriptorRegistry.getInstance(_BigObject2K1D.class);
		boolean eq = a.equals(b);

		assertTrue(eq);
		assertEquals(a.getKeyFields(), b.getKeyFields());
		assertEquals(a.getDiscriminatorFields(), b.getDiscriminatorFields());
		assertEquals(a.getValueFields(), b.getValueFields());
	}


	@Test
	public void testEquals2() throws IOException, ClassNotFoundException
	{
		EntityDescriptor a = EntityDescriptorRegistry.getInstance(_BigObject2K1D.class);
		EntityDescriptor b = null;
		boolean eq = a.equals(b);

		assertFalse(eq);
	}


	@Test
	public void testGetName() throws IOException, ClassNotFoundException
	{
		EntityDescriptor a = EntityDescriptorRegistry.getInstance(_BigObject2K1D.class);

		assertEquals(a.getName(), _BigObject2K1D.class.getName());
	}


	@Test
	public void testGetType() throws IOException, ClassNotFoundException
	{
		EntityDescriptor a = EntityDescriptorRegistry.getInstance(_BigObject2K1D.class);

		assertEquals(a.getType(), _BigObject2K1D.class);
	}


	@Test
	public void testHashCode() throws IOException, ClassNotFoundException
	{
		EntityDescriptor a = EntityDescriptorRegistry.getInstance(_BigObject2K1D.class);
		EntityDescriptor b = EntityDescriptorRegistry.getInstance(_BigObject2K1D.class);

		assertEquals(a.hashCode(), b.hashCode());
		assertEquals(a.getValueFields()[0].hashCode(), b.getValueFields()[0].hashCode());
	}


	@Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Entity has no keys.*")
	public void testInitializeFieldTypeLists() throws IOException, ClassNotFoundException
	{
		EntityDescriptorRegistry.getInstance(String.class);
	}
	
	
	@Test
	public void testGetJavaDeclaration()
	{
		EntityDescriptor ed = EntityDescriptorRegistry.getInstance(_BigObject2K1D.class);

		String str = ed.getJavaDeclaration().replace("\r\n","\n").replace("\n\n","\n");

		assertEquals(str, "package tests;\n" +
			"class _BigObject2K1D\n" +
			"{\n" +
			"	@Key Long _key1;\n" +
			"	@Key Long _key2;\n" +
			"	@Discriminator Long _discriminator;\n" +
			"	boolean mBoolean;\n" +
			"	byte mByte;\n" +
			"	short mShort;\n" +
			"	char mChar;\n" +
			"	int mInt;\n" +
			"	long mLong;\n" +
			"	float mFloat;\n" +
			"	double mDouble;\n" +
			"	String mString;\n" +
			"	Date mDate;\n" +
			"	boolean[] mBooleans;\n" +
			"	byte[] mBytes;\n" +
			"	short[] mShorts;\n" +
			"	char[] mChars;\n" +
			"	int[] mInts;\n" +
			"	long[] mLongs;\n" +
			"	float[] mFloats;\n" +
			"	double[] mDoubles;\n" +
			"	String[] mStrings;\n" +
			"	Date[] mDates;\n" +
			"	boolean[][] mBooleans2;\n" +
			"	byte[][] mBytes2;\n" +
			"	short[][] mShorts2;\n" +
			"	char[][] mChars2;\n" +
			"	int[][] mInts2;\n" +
			"	long[][] mLongs2;\n" +
			"	float[][] mFloats2;\n" +
			"	double[][] mDoubles2;\n" +
			"	String[][] mStrings2;\n" +
			"	Date[][] mDates2;\n" +
			"	Boolean mBooleanB;\n" +
			"	Byte mByteB;\n" +
			"	Short mShortB;\n" +
			"	Character mCharB;\n" +
			"	Integer mIntB;\n" +
			"	Long mLongB;\n" +
			"	Float mFloatB;\n" +
			"	Double mDoubleB;\n" +
			"	Boolean[] mBooleansB;\n" +
			"	Byte[] mBytesB;\n" +
			"	Short[] mShortsB;\n" +
			"	Character[] mCharsB;\n" +
			"	Integer[] mIntsB;\n" +
			"	Long[] mLongsB;\n" +
			"	Float[] mFloatsB;\n" +
			"	Double[] mDoublesB;\n" +
			"	Boolean[][] mBooleans2B;\n" +
			"	Byte[][] mBytes2B;\n" +
			"	Short[][] mShorts2B;\n" +
			"	Character[][] mChars2B;\n" +
			"	Integer[][] mInts2B;\n" +
			"	Long[][] mLongs2B;\n" +
			"	Float[][] mFloats2B;\n" +
			"	Double[][] mDoubles2B;\n" +
			"	Object mArrayList;\n" +
			"}");
	}
}

package org.terifan.raccoon;

import java.io.IOException;
import org.terifan.raccoon.serialization.EntityDescriptor;
import org.terifan.raccoon.serialization.EntityDescriptorFactory;
import static org.testng.Assert.*;
import org.testng.annotations.Test;
import resources.entities._BigObject2K1D;
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


	@Test
	public void testGetJavaDeclaration()
	{
		TableMetadata metadata = new TableMetadata(_BigObject2K1D.class, null);

		String str = metadata.getJavaDeclaration().replace("\r\n","\n").replace("\n\n","\n");

		assertEquals(str, "package resources.entities;\n" +
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


	@Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Entity has no keys.*")
	public void testNoKeys() throws IOException, ClassNotFoundException
	{
		new TableMetadata(String.class, null);
	}
}

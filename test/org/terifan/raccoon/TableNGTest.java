package org.terifan.raccoon;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;
import org.terifan.raccoon.serialization.FieldDescriptor;
import static org.testng.Assert.*;
import org.testng.annotations.Test;
import resources.entities._Animal1K;
import resources.entities._BigObject2K1D;
import resources.entities._Fruit1K1D;


public class TableNGTest
{
	public TableNGTest()
	{
	}


	@Test
	public void testEquals()
	{
		Table table1 = new Table(null, _Fruit1K1D.class, new DiscriminatorType(new _Fruit1K1D("red")));

		Table table2 = new Table(null, _Fruit1K1D.class, new DiscriminatorType(new _Fruit1K1D("red")));

		assertEquals(table1, table2);
	}


	@Test
	public void testToString()
	{
		Table metadata = new Table(null, _Fruit1K1D.class, new DiscriminatorType(new _Fruit1K1D("red")));

		assertEquals("resources.entities._Fruit1K1D[_color=red]", metadata.toString());
	}


	@Test
	public void testDiscriminatorDescription()
	{
		Table metadata = new Table(null, _Fruit1K1D.class, new DiscriminatorType(new _Fruit1K1D("red")));

		assertEquals("_color=red", metadata.getDiscriminatorDescription());
	}


	@Test
	public void testGetJavaDeclaration()
	{
		Table metadata = new Table(null, _BigObject2K1D.class, null);

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
		new Table(null, String.class, null);
	}


	@Test
	public void testResultSetFields() throws IOException
	{
		MemoryBlockDevice device = new MemoryBlockDevice(512);

		try (Database database = Database.open(device, OpenOption.CREATE))
		{
			database.save(new _Animal1K("cat", 1));
			database.save(new _Animal1K("dog", 2));
			database.save(new _Animal1K("horse", 3));
			database.save(new _Animal1K("cow", 4));
			database.commit();

			Table table = database.getTable(_Animal1K.class);
			FieldDescriptor[] fields = table.getFields();

			table.forEachResultSet(e->{
				for (int i = 0; i < fields.length; i++)
				{
					assertEquals(e.getField(i), fields[i]);
				}
			});
		}
	}


	@Test
	public void testList() throws IOException
	{
		MemoryBlockDevice device = new MemoryBlockDevice(512);

		try (Database database = Database.open(device, OpenOption.CREATE))
		{
			database.save(new _Animal1K("cat", 1));
			database.save(new _Animal1K("dog", 2));
			database.save(new _Animal1K("horse", 3));
			database.save(new _Animal1K("cow", 4));
			database.commit();

			HashSet<String> nameLookup = new HashSet<>(Arrays.asList("cat","dog","horse","cow"));
			HashSet<Integer> numberLookup = new HashSet<>(Arrays.asList(1,2,3,4));

			Table table = database.getTable(_Animal1K.class);

			table.forEachResultSet(e->{
				assertTrue(nameLookup.remove((String)e.get("_name")));
				assertTrue(numberLookup.remove((int)e.get("number")));
			});

			assertEquals(nameLookup.size(), 0);
		}
	}


	@Test
	public void testForEach() throws IOException
	{
		MemoryBlockDevice device = new MemoryBlockDevice(512);

		try (Database database = Database.open(device, OpenOption.CREATE))
		{
			database.save(new _Animal1K("cat", 1));
			database.save(new _Animal1K("dog", 2));
			database.save(new _Animal1K("horse", 3));
			database.save(new _Animal1K("cow", 4));
			database.commit();

			HashSet<String> nameLookup = new HashSet<>(Arrays.asList("cat","dog","horse","cow"));
			HashSet<Integer> numberLookup = new HashSet<>(Arrays.asList(1,2,3,4));

			database.getTable(_Animal1K.class).forEach(e->{
				assertTrue(nameLookup.remove(e._name));
				assertTrue(numberLookup.remove(e.number));
			});

			assertEquals(nameLookup.size(), 0);
		}
	}


	@Test
	public void testForEachCloseOnException() throws IOException
	{
		MemoryBlockDevice device = new MemoryBlockDevice(512);

		try (Database database = Database.open(device, OpenOption.CREATE))
		{
			database.save(new _Animal1K("cat", 1));
			database.save(new _Animal1K("dog", 2));
			database.save(new _Animal1K("horse", 3));
			database.save(new _Animal1K("cow", 4));
			database.commit();

			Table<_Animal1K> table = database.getTable(_Animal1K.class);

			try
			{
				table.forEach(e->{
					throw new IllegalStateException();
				});

				fail("an exception was not thrown");
			}
			catch (IllegalStateException e)
			{
				// ignore
			}

			assertFalse(table.isReadLocked());
		}
	}
}

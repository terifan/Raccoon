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
import resources.entities._ClassNoKeys;
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

		String expected = "package resources.entities;\n" +
			"@Entity\n" +
			"class _BigObject2K1D\n" +
			"{\n" +
			"	@Id Long _key1;\n" +
			"	@Id Long _key2;\n" +
			"	@Discriminator Long _discriminator;\n" +
			"	@Column boolean mBoolean;\n" +
			"	@Column byte mByte;\n" +
			"	@Column short mShort;\n" +
			"	@Column char mChar;\n" +
			"	@Column int mInt;\n" +
			"	@Column long mLong;\n" +
			"	@Column float mFloat;\n" +
			"	@Column double mDouble;\n" +
			"	@Column String mString;\n" +
			"	@Column java.util.Date mDate;\n" +
			"	@Column boolean[] mBooleans;\n" +
			"	@Column byte[] mBytes;\n" +
			"	@Column short[] mShorts;\n" +
			"	@Column char[] mChars;\n" +
			"	@Column int[] mInts;\n" +
			"	@Column long[] mLongs;\n" +
			"	@Column float[] mFloats;\n" +
			"	@Column double[] mDoubles;\n" +
			"	@Column String[] mStrings;\n" +
			"	@Column java.util.Date[] mDates;\n" +
			"	@Column boolean[][] mBooleans2;\n" +
			"	@Column byte[][] mBytes2;\n" +
			"	@Column short[][] mShorts2;\n" +
			"	@Column char[][] mChars2;\n" +
			"	@Column int[][] mInts2;\n" +
			"	@Column long[][] mLongs2;\n" +
			"	@Column float[][] mFloats2;\n" +
			"	@Column double[][] mDoubles2;\n" +
			"	@Column String[][] mStrings2;\n" +
			"	@Column java.util.Date[][] mDates2;\n" +
			"	@Column Boolean mBooleanB;\n" +
			"	@Column Byte mByteB;\n" +
			"	@Column Short mShortB;\n" +
			"	@Column Character mCharB;\n" +
			"	@Column Integer mIntB;\n" +
			"	@Column Long mLongB;\n" +
			"	@Column Float mFloatB;\n" +
			"	@Column Double mDoubleB;\n" +
			"	@Column Boolean[] mBooleansB;\n" +
			"	@Column Byte[] mBytesB;\n" +
			"	@Column Short[] mShortsB;\n" +
			"	@Column Character[] mCharsB;\n" +
			"	@Column Integer[] mIntsB;\n" +
			"	@Column Long[] mLongsB;\n" +
			"	@Column Float[] mFloatsB;\n" +
			"	@Column Double[] mDoublesB;\n" +
			"	@Column Boolean[][] mBooleans2B;\n" +
			"	@Column Byte[][] mBytes2B;\n" +
			"	@Column Short[][] mShorts2B;\n" +
			"	@Column Character[][] mChars2B;\n" +
			"	@Column Integer[][] mInts2B;\n" +
			"	@Column Long[][] mLongs2B;\n" +
			"	@Column Float[][] mFloats2B;\n" +
			"	@Column Double[][] mDoubles2B;\n" +
			"	@Column java.util.List mArrayList;\n" +
			"}";

		assertEquals(str, expected);
	}


	@Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Entity has no keys.*")
	public void testNoKeys() throws IOException, ClassNotFoundException
	{
		new Table(null, _ClassNoKeys.class, null);
	}


	@Test
	public void testResultSetFields() throws IOException
	{
		MemoryBlockDevice device = new MemoryBlockDevice(512);

		try (Database database = new Database(device, DatabaseOpenOption.CREATE))
		{
			database.save(new _Animal1K("cat", 1));
			database.save(new _Animal1K("dog", 2));
			database.save(new _Animal1K("horse", 3));
			database.save(new _Animal1K("cow", 4));
			database.commit();

			Table table = database.getTable(_Animal1K.class);
			FieldDescriptor[] fields = table.getFields();

			database.forEachResultSet(_Animal1K.class, e->{
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

		try (Database database = new Database(device, DatabaseOpenOption.CREATE))
		{
			database.save(new _Animal1K("cat", 1));
			database.save(new _Animal1K("dog", 2));
			database.save(new _Animal1K("horse", 3));
			database.save(new _Animal1K("cow", 4));
			database.commit();

			HashSet<String> nameLookup = new HashSet<>(Arrays.asList("cat","dog","horse","cow"));
			HashSet<Integer> numberLookup = new HashSet<>(Arrays.asList(1,2,3,4));

			database.forEachResultSet(_Animal1K.class, e->{
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

		try (Database database = new Database(device, DatabaseOpenOption.CREATE))
		{
			database.save(new _Animal1K("cat", 1));
			database.save(new _Animal1K("dog", 2));
			database.save(new _Animal1K("horse", 3));
			database.save(new _Animal1K("cow", 4));
			database.commit();

			HashSet<String> nameLookup = new HashSet<>(Arrays.asList("cat","dog","horse","cow"));
			HashSet<Integer> numberLookup = new HashSet<>(Arrays.asList(1,2,3,4));

			database.forEach(_Animal1K.class, e->{
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

		try (Database database = new Database(device, DatabaseOpenOption.CREATE))
		{
			database.save(new _Animal1K("cat", 1));
			database.save(new _Animal1K("dog", 2));
			database.save(new _Animal1K("horse", 3));
			database.save(new _Animal1K("cow", 4));
			database.commit();

			Table<_Animal1K> table = database.getTable(_Animal1K.class);

			try
			{
				database.forEach(_Animal1K.class, e->{
					throw new IllegalStateException();
				});

				fail("an exception was not thrown");
			}
			catch (IllegalStateException e)
			{
				// ignore
			}

//			assertFalse(table.isReadLocked());
		}
	}
}

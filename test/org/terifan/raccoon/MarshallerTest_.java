package org.terifan.raccoon;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Test;
import static org.junit.Assert.*;


public class MarshallerTest_
{
	public MarshallerTest_()
	{
	}


//	@Test
//	public void testSomeMethod1() throws Exception
//	{
//		TableSchema schema = new TableSchema(FilterTest.class, null);
//
//		FilterTest input = new FilterTest();
//		byte[] discriminators = schema.getDiscriminators(input);
//		byte[] keys = schema.getKeys(input);
//		byte[] values = schema.getValues(input);
//
//		FilterTest output = new FilterTest();
//		schema.update(output, discriminators);
//		schema.update(output, keys);
//		schema.update(output, values);
//
////		new ObjectSerializer().print(output, Log.out);
//
//		assertEquals(1, output.disc1);
//		assertEquals(2, output.disc2);
//		assertEquals(3, output.disc3);
//		assertEquals(4, output.key1);
//		assertEquals(5, output.key2);
//		assertEquals(6, output.key3);
//		assertEquals(7, output.value1);
//		assertEquals(8, output.value2);
//		assertEquals(9, output.value3);
//	}
	

	static class FilterTest
	{
		@Discriminator int disc1=1;
		@Discriminator int disc2=2;
		@Discriminator int disc3=3;
		@Key int key1=4;
		@Key int key2=5;
		@Key int key3=6;
		int value1=7;
		int value2=8;
		int value3=9;
	}


//	@Test
//	public void testSomeMethod() throws Exception
//	{
//		TableSchema schema = new TableSchema(Dummy.class, null);
//		Dummy input = new Dummy(true);
//
//		byte[] packet = schema.getValues(input);
//
//		Dummy output = new Dummy();
//		schema.update(output, packet);
//
//		assertEquals(input.b, output.b);
//		assertEquals(input.c, output.c);
//		assertEquals(input.d, output.d);
//		assertEquals(input.i, output.i);
//		assertEquals(input.s, output.s);
//		assertArrayEquals(input.bytes, output.bytes);
//		assertArrayEquals(input.dates.toArray(), output.dates.toArray());
//		assertArrayEquals(input.ints1, output.ints1);
//		assertArrayEquals(input.ints2, output.ints2);
//		assertArrayEquals(input.ints3, output.ints3);
//		assertArrayEquals(input.ints4, output.ints4);
//		assertArrayEquals(input.longs.toArray(), output.longs.toArray());
//		assertArrayEquals(input.map1.values().toArray(), output.map1.values().toArray());
//		assertArrayEquals(input.map2.values().toArray(), output.map2.values().toArray());
//		assertArrayEquals(input.map3.values().toArray(), output.map3.values().toArray());
//	}


	static class Dummy
	{
		String s;
		int i;
		int[] ints1;
		int[][] ints2;
		int[][][] ints3;
		int[][][][] ints4;
		Byte[] bytes;
		byte[] byteArray;
		char b;
		Character c;
		Double d;
		List<Long> longs;
		Set<Date> dates;
		HashMap<String, int[][]> map1;
		HashMap<String, Integer[]> map2;
		HashMap<String, Integer> map3;

		Dummy()
		{
		}


		Dummy(boolean xxx)
		{
			map3 = new HashMap<>();
			map2 = new HashMap<>();
			map1 = new HashMap<>();
			dates = new HashSet<>(Arrays.asList(new Date(), new Date(), new Date()));
			longs = new ArrayList<>(Arrays.asList(6541981934698L, 32134691615654L, -21984346645496L));
			d = Math.PI;
			c = 'x';
			b = 'y';
			bytes = new Byte[]{7, 8, 9};
			byteArray = new byte[1000];
			ints4 = new int[][][][]{{{{123}}}};
			ints3 = new int[][][]{{
				{31, 32, 33, 34, 35, 36},
				null,
				{37, 38, 39, 40, 41, 42, 43}
			}, null, {
				null
			}};
			ints2 = new int[][]{{21, 22, 23, 24}, null, {25, 26, 27, 28, 29}, {}};
			ints1 = new int[]{11, 12, 13};
			i = 6494;
			s = "hello world";
			map1.put("abc", new int[][]{{74,75,76},null,{77,78,79}});
			map2.put("def", new Integer[]{87,88,89});
			map3.put("ghi", 1);
			map3.put("jkl", 2);
		}
	}
}

package tests;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import org.terifan.raccoon.Key;
import static tests.__TestUtils.*;


public class _BigObject1K implements Serializable
{
	private transient static final long serialVersionUID = 1L;

	@Key public int _key;

	public boolean mBoolean;
	public byte mByte;
	public short mShort;
	public char mChar;
	public int mInt;
	public long mLong;
	public float mFloat;
	public double mDouble;
	public String mString;
	public Date mDate;

	public boolean[] mBooleans;
	public byte[] mBytes;
	public short[] mShorts;
	public char[] mChars;
	public int[] mInts;
	public long[] mLongs;
	public float[] mFloats;
	public double[] mDoubles;
	public String[] mStrings;
	public Date[] mDates;
	
	public int[][] mMatrix;
	public List<String> mArrayList;


	public _BigObject1K random()
	{
		_key = (int)System.nanoTime();
		
		mBoolean = x();
		mByte = b();
		mShort = s();
		mChar = c();
		mInt = i();
		mLong = l();
		mFloat = f();
		mDouble = d();
		mString = t();
		mDate = new Date();

		mBooleans = new boolean[]{x(), x(), x()};
		mBytes = new byte[]{b(),b(),b()};
		mShorts = new short[]{s(),s(),s()};
		mChars = new char[]{c(),c(),c()};
		mInts = new int[]{i(),i(),i()};
		mLongs = new long[]{l(),l(),l()};
		mFloats = new float[]{f(),f(),f()};
		mDoubles = new double[]{d(),d(),d()};
		mStrings = new String[]{t(),t(),t()};
		mDates = new Date[]{new Date(),new Date(),new Date()};

		mArrayList = Arrays.asList("arraylist");
		mMatrix = new int[][]{{i(),i()},{i()}};
		
		return this;
	}


	private static <T> HashMap<String,T> asMap(Class<T> aType, T... aValues)
	{
		HashMap<String,T> map = new HashMap<>();
		int i = 'a';
		for (T v : aValues)
		{
			map.put(""+i++, v);
		}
		return map;
	}


	private static <K,V> HashMap<K[],V[]> asMap2(Class<K> aKeyType, Class<V> aValueType, K[][] aKeys, V[][] aValues)
	{
		HashMap<K[],V[]> map = new HashMap<>();
		for (int i = 0; i < aKeys.length; i++)
		{
			map.put(aKeys[i], aValues[i]);
		}
		return map;
	}
}

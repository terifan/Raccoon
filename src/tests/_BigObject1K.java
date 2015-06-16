package tests;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import org.terifan.raccoon.Key;
import static tests.__TestUtils.*;


public class _BigObject1K implements Serializable
{
	private transient static final long serialVersionUID = 1L;

	@Key public int _key;

	public boolean mBoolean;
	public byte mByte;
	short mShort;
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

	public ArrayList<Boolean> mBooleanList;
	public ArrayList<Byte> mByteList;
	public ArrayList<Short> mShortList;
	public ArrayList<Character> mCharList;
	public ArrayList<Integer> mIntList;
	public ArrayList<Long> mLongList;
	public ArrayList<Float> mFloatList;
	public ArrayList<Double> mDoubleList;
	public ArrayList<String> mStringList;
	public ArrayList<Date> mDateList;

	public HashSet<Boolean> mBooleanSet;
	public HashSet<Byte> mByteSet;
	public HashSet<Short> mShortSet;
	public HashSet<Character> mCharSet;
	public HashSet<Integer> mIntSet;
	public HashSet<Long> mLongSet;
	public HashSet<Float> mFloatSet;
	public HashSet<Double> mDoubleSet;
	public HashSet<String> mStringSet;
	public HashSet<Date> mDateSet;

	public HashMap<String,Boolean> mBooleanMap;
	public HashMap<String,Byte> mByteMap;
	public HashMap<String,Short> mShortMap;
	public HashMap<String,Character> mCharMap;
	public HashMap<String,Integer> mIntMap;
	public HashMap<String,Long> mLongMap;
	public HashMap<String,Float> mFloatMap;
	public HashMap<String,Double> mDoubleMap;
	public HashMap<String,String> mStringMap;
	public HashMap<String,Date> mDateMap;

	public ArrayList<String[]> mStringListArray;
	public HashMap<String[],String[]> mStringMapArray;

//	HashMap<String,ArrayList<String>> mStringMapList;


	public _BigObject1K random()
	{
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

		mBooleanList = new ArrayList<>(Arrays.asList(x(), x(), x()));
		mByteList = new ArrayList<>(Arrays.asList(b(),b(),b()));
		mShortList = new ArrayList<>(Arrays.asList(s(),s(),s()));
		mCharList = new ArrayList<>(Arrays.asList(c(),c(),c()));
		mIntList = new ArrayList<>(Arrays.asList(i(),i(),i()));
		mLongList = new ArrayList<>(Arrays.asList(l(),l(),l()));
		mFloatList = new ArrayList<>(Arrays.asList(f(),f(),f()));
		mDoubleList = new ArrayList<>(Arrays.asList(d(),d(),d()));
		mStringList = new ArrayList<>(Arrays.asList(t(),t(),t()));
		mDateList = new ArrayList<>(Arrays.asList(new Date(),new Date(),new Date()));

		mBooleanSet = new HashSet<>(Arrays.asList(x(), x(), x()));
		mByteSet = new HashSet<>(Arrays.asList(b(),b(),b()));
		mShortSet = new HashSet<>(Arrays.asList(s(),s(),s()));
		mCharSet = new HashSet<>(Arrays.asList(c(),c(),c()));
		mIntSet = new HashSet<>(Arrays.asList(i(),i(),i()));
		mLongSet = new HashSet<>(Arrays.asList(l(),l(),l()));
		mFloatSet = new HashSet<>(Arrays.asList(f(),f(),f()));
		mDoubleSet = new HashSet<>(Arrays.asList(d(),d(),d()));
		mStringSet = new HashSet<>(Arrays.asList(t(),t(),t()));
		mDateSet = new HashSet<>(Arrays.asList(new Date(),new Date(),new Date()));

		mBooleanMap = asMap(Boolean.class, x(), x(), x());
		mByteMap = asMap(Byte.class, b(),b(),b());
		mShortMap = asMap(Short.class, s(),s(),s());
		mCharMap = asMap(Character.class, c(),c(),c());
		mIntMap = asMap(Integer.class, i(),i(),i());
		mLongMap = asMap(Long.class, l(),l(),l());
		mFloatMap = asMap(Float.class, f(),f(),f());
		mDoubleMap = asMap(Double.class, d(),d(),d());
		mStringMap = asMap(String.class, t(),t(),t());
		mDateMap = asMap(Date.class, new Date(),new Date(),new Date());

		mStringListArray = new ArrayList<>(Arrays.asList(new String[]{t(),t(),t()}, new String[]{t(),null,"abcdefghijklmnop"}));
		mStringMapArray = asMap2(String.class, String.class, new String[][]{{t(), t(), t()}}, new String[][]{{t(), t(), t()}});

//		mStringMapList = new HashMap<>();
//		mStringMapList.put(t(), new ArrayList<>(Arrays.asList(t(),t(),t())));

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

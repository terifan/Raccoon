package sample;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import static sample.Utils.*;


public class _BigObject implements Serializable
{
	private transient static final long serialVersionUID = 1L;

	boolean mBoolean;
	byte mByte;
	short mShort;
	char mChar;
	int mInt;
	long mLong;
	float mFloat;
	double mDouble;
	String mString;
	Date mDate;

	boolean[] mBooleans;
	byte[] mBytes;
	short[] mShorts;
	char[] mChars;
	int[] mInts;
	long[] mLongs;
	float[] mFloats;
	double[] mDoubles;
	String[] mStrings;
	Date[] mDates;

	ArrayList<Boolean> mBooleanList;
	ArrayList<Byte> mByteList;
	ArrayList<Short> mShortList;
	ArrayList<Character> mCharList;
	ArrayList<Integer> mIntList;
	ArrayList<Long> mLongList;
	ArrayList<Float> mFloatList;
	ArrayList<Double> mDoubleList;
	ArrayList<String> mStringList;
	ArrayList<Date> mDateList;

	HashSet<Boolean> mBooleanSet;
	HashSet<Byte> mByteSet;
	HashSet<Short> mShortSet;
	HashSet<Character> mCharSet;
	HashSet<Integer> mIntSet;
	HashSet<Long> mLongSet;
	HashSet<Float> mFloatSet;
	HashSet<Double> mDoubleSet;
	HashSet<String> mStringSet;
	HashSet<Date> mDateSet;

	HashMap<String,Boolean> mBooleanMap;
	HashMap<String,Byte> mByteMap;
	HashMap<String,Short> mShortMap;
	HashMap<String,Character> mCharMap;
	HashMap<String,Integer> mIntMap;
	HashMap<String,Long> mLongMap;
	HashMap<String,Float> mFloatMap;
	HashMap<String,Double> mDoubleMap;
	HashMap<String,String> mStringMap;
	HashMap<String,Date> mDateMap;

	ArrayList<String[]> mStringListArray;
	HashMap<String[],String[]> mStringMapArray;

//	HashMap<String,ArrayList<String>> mStringMapList;


	_BigObject random()
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

package org.terifan.raccoon.serialization;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import static org.terifan.raccoon.serialization.Utils.*;


public class MsgBasicTypes implements Serializable
{
	private static final long serialVersionUID = 1L;

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


	MsgBasicTypes random()
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

		return this;
	}


	private static <T> HashMap<String,T> asMap(Class<T> aType, T... aValues)
	{
		HashMap<String,T> map = new HashMap<>();
		map.put("a", (T)aValues[0]);
		map.put("b", (T)aValues[1]);
		map.put("c", (T)aValues[2]);
		return map;
	}
}

package resources.entities;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import org.terifan.raccoon.Discriminator;
import org.terifan.raccoon.Key;
import static resources.__TestUtils.*;


public class _BigObject2K1D implements Serializable
{
	private transient static final long serialVersionUID = 1L;

	@Key public Long _key1;
	@Key public Long _key2;

	@Discriminator public Long _discriminator;

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

	public boolean[][] mBooleans2;
	public byte[][] mBytes2;
	public short[][] mShorts2;
	public char[][] mChars2;
	public int[][] mInts2;
	public long[][] mLongs2;
	public float[][] mFloats2;
	public double[][] mDoubles2;
	public String[][] mStrings2;
	public Date[][] mDates2;

	public Boolean mBooleanB;
	public Byte mByteB;
	public Short mShortB;
	public Character mCharB;
	public Integer mIntB;
	public Long mLongB;
	public Float mFloatB;
	public Double mDoubleB;

	public Boolean[] mBooleansB;
	public Byte[] mBytesB;
	public Short[] mShortsB;
	public Character[] mCharsB;
	public Integer[] mIntsB;
	public Long[] mLongsB;
	public Float[] mFloatsB;
	public Double[] mDoublesB;

	public Boolean[][] mBooleans2B;
	public Byte[][] mBytes2B;
	public Short[][] mShorts2B;
	public Character[][] mChars2B;
	public Integer[][] mInts2B;
	public Long[][] mLongs2B;
	public Float[][] mFloats2B;
	public Double[][] mDoubles2B;

	public List<String> mArrayList;


	public _BigObject2K1D random()
	{
		_key1 = System.nanoTime();
		_key2 = System.nanoTime();
		_discriminator = System.nanoTime();

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

		mBooleans = new boolean[]{x(),x(),x()};
		mBytes = new byte[]{b(),b(),b()};
		mShorts = new short[]{s(),s(),s()};
		mChars = new char[]{c(),c(),c()};
		mInts = new int[]{i(),i(),i()};
		mLongs = new long[]{l(),l(),l()};
		mFloats = new float[]{f(),f(),f()};
		mDoubles = new double[]{d(),d(),d()};
		mStrings = new String[]{t(),t(),t()};
		mDates = new Date[]{new Date(),new Date(),new Date()};

		mBooleans2 = new boolean[][]{{x(),x(),x()}, null, {x(),x(),x()}, {}};
		mBytes2 = new byte[][]{{b(),b(),b()}, null, {b(),b(),b()}, {}};
		mShorts2 = new short[][]{{s(),s(),s()}, null, {s(),s(),s()}, {}};
		mChars2 = new char[][]{{c(),c(),c()}, null, {c(),c(),c()}, {}};
		mInts2 = new int[][]{{i(),i(),i()}, null, {i(),i(),i()}, {}};
		mLongs2 = new long[][]{{l(),l(),l()}, null, {l(),l(),l()}, {}};
		mFloats2 = new float[][]{{f(),f(),f()}, null, {f(),f(),f()}, {}};
		mDoubles2 = new double[][]{{d(),d(),d()}, null, {d(),d(),d()}, {}};
		mStrings2 = new String[][]{{t(),t(),t()}, null, {t(),t(),t()}, {}};
		mDates2 = new Date[][]{{new Date(),new Date(),new Date()}, {new Date(),new Date(),new Date()}, {}};

		mBooleanB = x();
		mByteB = b();
		mShortB = s();
		mCharB = c();
		mIntB = i();
		mLongB = l();
		mFloatB = f();
		mDoubleB = d();

		mBooleansB = new Boolean[]{x(),x(),null,x()};
		mBytesB = new Byte[]{b(),b(), null,b()};
		mShortsB = new Short[]{s(),s(), null,s()};
		mCharsB = new Character[]{c(),c(), null,c()};
		mIntsB = new Integer[]{i(),i(), null,i()};
		mLongsB = new Long[]{l(),l(), null,l()};
		mFloatsB = new Float[]{f(),f(), null,f()};
		mDoublesB = new Double[]{d(),d(), null,d()};

		mBooleans2B = new Boolean[][]{{x(),x(),x()}, null, {x(),x(), null,x()}, {}};
		mBytes2B = new Byte[][]{{b(),b(),b()}, null, {b(),b(), null,b()}, {}};
		mShorts2B = new Short[][]{{s(),s(),s()}, null, {s(),s(), null,s()}, {}};
		mChars2B = new Character[][]{{c(),c(),c()}, null, {c(),c(), null,c()}, {}};
		mInts2B = new Integer[][]{{i(),i(),i()}, null, {i(),i(), null,i()}, {}};
		mLongs2B = new Long[][]{{l(),l(),l()}, null, {l(),l(), null,l()}, {}};
		mFloats2B = new Float[][]{{f(),f(),f()}, null, {f(),f(), null,f()}, {}};
		mDoubles2B = new Double[][]{{d(),d(),d()}, null, {d(),d(), null,d()}, {}};

		mArrayList = Arrays.asList("arraylist");

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

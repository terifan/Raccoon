package org.terifan.v1.raccoon.security;

import java.util.Arrays;


public final class Elephant
{
	private final static int BYTES_PER_CIPHER_BLOCK = 16;
	private final static int WORDS_PER_CIPHER_BLOCK = 4;

	private final static int [] ROTATE1 = {9,0,13,0};
	private final static int [] ROTATE2 = {0,10,0,25};

	private final transient int [] mTweakKey = new int[8];
	private final transient int [] mWords;
	private final transient int [] mIV = new int[WORDS_PER_CIPHER_BLOCK];
	private final transient int [] mTemp = new int[WORDS_PER_CIPHER_BLOCK];
	private final transient int mUnitSize;
	private final transient int mWordsPerUnit;
	private final transient int mWordMask;
	private final transient int mBlocksPerUnit;


	public Elephant(int aUnitSize)
	{
		if (aUnitSize < 32)
		{
			throw new IllegalArgumentException("Unit size must be greater or equal to 32.");
		}
		if ((aUnitSize & -aUnitSize) != aUnitSize)
		{
			throw new IllegalArgumentException("Unit size must be power of 2.");
		}

		mUnitSize = aUnitSize;
		mWordsPerUnit = mUnitSize / 4;
		mWords = new int[mWordsPerUnit];
		mWordMask = mWordsPerUnit - 1;
		mBlocksPerUnit = mUnitSize / BYTES_PER_CIPHER_BLOCK;
	}


	public void reset()
	{
		Arrays.fill(mIV, 0);
		Arrays.fill(mTemp, 0);
		Arrays.fill(mWords, 0);
		Arrays.fill(mTweakKey, 0);
	}


	public synchronized void encrypt(byte [] aBuffer, int aOffset, int aLength, long aStartDataUnitNo, Cipher aCipher, Cipher aTweakCipher, int [] aIV, int [] aTweakKey, long aExtraTweak)
	{
		for (int unitIndex = 0, offset = aOffset, numDataUnits = aLength / mUnitSize; unitIndex < numDataUnits; unitIndex++, offset += mUnitSize)
		{
			toInts(aBuffer, offset, mWords, mWordsPerUnit);

			// encryption cbc mode

			prepareIV(aStartDataUnitNo + unitIndex, aIV, mIV, aTweakCipher);

			for (int i = 0; i < mWordsPerUnit; i += WORDS_PER_CIPHER_BLOCK)
			{
				for (int j = 0; j < WORDS_PER_CIPHER_BLOCK; j++)
				{
					mIV[j] ^= mWords[i + j];
				}

				aCipher.engineEncryptBlock(mIV, 0, mIV, 0);

				System.arraycopy(mIV, 0, mWords, i, WORDS_PER_CIPHER_BLOCK);
			}

			// elephant diffuser

			prepareTweak(aStartDataUnitNo + unitIndex, aTweakKey, mTweakKey, aTweakCipher, aExtraTweak);

			for (int i = 0; i < mBlocksPerUnit; i++)
			{
				mWords[i] ^= mTweakKey[i & 7] ^ i;
			}

			for (int i = 5 * mWordsPerUnit; --i >= 0;)
			{
				mWords[i & mWordMask] -= (mWords[(i + 2) & mWordMask] ^ rol(mWords[(i + 5) & mWordMask], ROTATE2[i & 3]));
			}

			for (int i = 3 * mWordsPerUnit; --i >= 0;)
			{
				mWords[i & mWordMask] -= (mWords[(i - 2) & mWordMask] ^ rol(mWords[(i - 5) & mWordMask], ROTATE1[i & 3]));
			}

			toBytes(mWords, offset, aBuffer, mWordsPerUnit);
		}
	}


	public synchronized void decrypt(byte [] aBuffer, int aOffset, int aLength, long aStartDataUnitNo, Cipher aCipher, Cipher aTweakCipher, int [] aIV, int [] aTweakKey, long aExtraTweak)
	{
		for (int unitIndex = 0, offset = aOffset, numDataUnits = aLength / mUnitSize; unitIndex < numDataUnits; unitIndex++, offset += mUnitSize)
		{
			toInts(aBuffer, offset, mWords, mWordsPerUnit);

			// elephant diffuser

			prepareTweak(aStartDataUnitNo + unitIndex, aTweakKey, mTweakKey, aTweakCipher, aExtraTweak);

			for (int i = 0; i < 3 * mWordsPerUnit; i++)
			{
				mWords[i & mWordMask] += (mWords[(i - 2) & mWordMask] ^ rol(mWords[(i - 5) & mWordMask], ROTATE1[i & 3]));
			}

			for (int i = 0; i < 5 * mWordsPerUnit; i++)
			{
				mWords[i & mWordMask] += (mWords[(i + 2) & mWordMask] ^ rol(mWords[(i + 5) & mWordMask], ROTATE2[i & 3]));
			}

			for (int i = 0; i < mBlocksPerUnit; i++)
			{
				mWords[i] ^= mTweakKey[i & 7] ^ i;
			}

			// decryption cbc mode

			prepareIV(aStartDataUnitNo + unitIndex, aIV, mIV, aTweakCipher);

			for (int i = 0; i < mWordsPerUnit; i += WORDS_PER_CIPHER_BLOCK)
			{
				System.arraycopy(mWords, i, mTemp, 0, WORDS_PER_CIPHER_BLOCK);

				aCipher.engineDecryptBlock(mWords, i, mWords, i);

				for (int j = 0; j < WORDS_PER_CIPHER_BLOCK; j++)
				{
					mWords[i+j] ^= mIV[j];
				}

				System.arraycopy(mTemp, 0, mIV, 0, WORDS_PER_CIPHER_BLOCK);
			}

			toBytes(mWords, offset, aBuffer, mWordsPerUnit);
		}
	}


	private void prepareIV(long aDataUnitNo, int [] aInputIV, int [] aOutputIV, Cipher aTweakCipher)
	{
		aOutputIV[0] = aInputIV[0];
		aOutputIV[1] = aInputIV[1];
		aOutputIV[2] = aInputIV[2] + (int)(aDataUnitNo >>> 32);
		aOutputIV[3] = aInputIV[3] + (int)(aDataUnitNo);

		aTweakCipher.engineEncryptBlock(aOutputIV, 0, aOutputIV, 0);
	}


	private void prepareTweak(long aDataUnitNo, int[] aInputTweak, int[] aOutputTweak, Cipher aTweakCipher, long aExtraTweak)
	{
		aOutputTweak[0] = aInputTweak[0] ^ (int)(aExtraTweak >>> 32);
		aOutputTweak[1] = aInputTweak[1];
		aOutputTweak[2] = aInputTweak[2] + (int)(aDataUnitNo >>> 32);
		aOutputTweak[3] = aInputTweak[3] + (int)(aDataUnitNo);

		aOutputTweak[4] = aInputTweak[4] ^ (int)(aExtraTweak);
		aOutputTweak[5] = aInputTweak[5];
		aOutputTweak[6] = aInputTweak[6] + (int)(aDataUnitNo >>> 32);
		aOutputTweak[7] = aInputTweak[7] + (int)(aDataUnitNo);

		aTweakCipher.engineEncryptBlock(aOutputTweak, 0, aOutputTweak, 0);
		aTweakCipher.engineEncryptBlock(aOutputTweak, 4, aOutputTweak, 4);
	}


	private int rol(int i, int distance)
	{
		return (i << distance) | (i >>> -distance);
	}


	private static void toInts(byte [] aInput, int aOffset, int [] aOutput, int aNumWords)
	{
		for (int i = 0; i < aNumWords; i++)
		{
			aOutput[i] = (int)(((255 & aInput[aOffset++]) << 24)
					   +       ((255 & aInput[aOffset++]) << 16)
					   +       ((255 & aInput[aOffset++]) <<  8)
					   +       ((255 & aInput[aOffset++])      ));
		}
	}


	private static void toBytes(int [] aInput, int aOffset, byte [] aOutput, int aNumWords)
	{
		for (int i = 0; i < aNumWords; i++)
		{
			int v = aInput[i];
			aOutput[aOffset++] = (byte)(v >>> 24);
			aOutput[aOffset++] = (byte)(v >>  16);
			aOutput[aOffset++] = (byte)(v >>   8);
			aOutput[aOffset++] = (byte)(v       );
		}
	}


//	public static void main(String... args)
//	{
//		try
//		{
//			int L = 512;
//			Elephant instance = new Elephant(L);
//
//			byte [] cipherKey = new byte[32];
//			byte [] tweakKey = new byte[32];
//
//			Random rnd = new Random(1);
//			rnd.nextBytes(cipherKey);
//			rnd.nextBytes(tweakKey);
//
//			Cipher cipher = new Twofish(new SecretKey(cipherKey));
//			Cipher tweakCipher = new Twofish(new SecretKey(tweakKey));
//
//			for (int test = 0; test < 1; test++)
//			{
//				int [] iv = new int[4];
//				int [] tweak = new int[8];
//
//				long unitIndex = rnd.nextLong();
//				iv[0] = rnd.nextInt();
//				iv[1] = rnd.nextInt();
//				iv[2] = rnd.nextInt();
//				iv[3] = rnd.nextInt();
//				tweak[0] = rnd.nextInt();
//				tweak[1] = rnd.nextInt();
//				tweak[2] = rnd.nextInt();
//				tweak[3] = rnd.nextInt();
//				tweak[4] = rnd.nextInt();
//				tweak[5] = rnd.nextInt();
//				tweak[6] = rnd.nextInt();
//				tweak[7] = rnd.nextInt();
//
//				byte [] clearText = new byte[3*L];
//				clearText[0] = (byte)test;
//
//				byte [] encoded = clearText.clone();
//				instance.encrypt(encoded, 0, 3*L, unitIndex+0, cipher, tweakCipher, iv, tweak, 0, 0);
//
//				byte [] decoded = encoded.clone();
//				instance.decrypt(decoded, 0, 1*L, unitIndex+0, cipher, tweakCipher, iv, tweak, 0, 0);
//				instance.decrypt(decoded, L, 2*L, unitIndex+1, cipher, tweakCipher, iv, tweak, 0, 0);
//
//				//System.out.println("CLEARTEXT");
//				//Debug.hexDump(clearText);
//				System.out.println("ENCODED");
//				Debug.hexDump(encoded);
//				System.out.println("DECODED");
//				Debug.hexDump(decoded);
//				System.out.println(Arrays.equals(clearText, decoded));
//			}
//		}
//		catch (Exception e)
//		{
//			e.printStackTrace(System.out);
//		}
//	}
}
package org.terifan.raccoon.security;


public interface Crypto
{
	void decrypt(int aUnitSize, byte[] aInput, byte[] aOutput, int aOffset, int aLength, long aStartDataUnitNo, int[] aIV, Cipher aCipher, Cipher aTweakCipher, int[] aTweakKey, long aExtraTweak);

	void encrypt(int aUnitSize, byte[] aInput, byte[] aOutput, int aOffset, int aLength, long aStartDataUnitNo, int[] aIV, Cipher aCipher, Cipher aTweakCipher, int[] aTweakKey, long aExtraTweak);

	void reset();
}

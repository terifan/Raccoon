package org.terifan.security.cryptography;

import java.util.Arrays;


/**
 * AES (Rijndael) is a variable block-size and variable key-size symmetric cipher.
 *
 * <p>
 * References:</p>
 *
 * <ol>
 * <li><a href="http://www.esat.kuleuven.ac.be/~rijmen/rijndael/">The Rijndael Block Cipher - AES Proposal</a>.<br>
 * <a href="mailto:vincent.rijmen@esat.kuleuven.ac.be">Vincent Rijmen</a> and
 * <a href="mailto:daemen.j@protonworld.com">Joan Daemen</a>.</li>
 * </ol>
 */
public final class AES implements BlockCipher
{
	private static final String SS
		= "\u637C\u777B\uF26B\u6FC5\u3001\u672B\uFED7\uAB76"
		+ "\uCA82\uC97D\uFA59\u47F0\uADD4\uA2AF\u9CA4\u72C0"
		+ "\uB7FD\u9326\u363F\uF7CC\u34A5\uE5F1\u71D8\u3115"
		+ "\u04C7\u23C3\u1896\u059A\u0712\u80E2\uEB27\uB275"
		+ "\u0983\u2C1A\u1B6E\u5AA0\u523B\uD6B3\u29E3\u2F84"
		+ "\u53D1\u00ED\u20FC\uB15B\u6ACB\uBE39\u4A4C\u58CF"
		+ "\uD0EF\uAAFB\u434D\u3385\u45F9\u027F\u503C\u9FA8"
		+ "\u51A3\u408F\u929D\u38F5\uBCB6\uDA21\u10FF\uF3D2"
		+ "\uCD0C\u13EC\u5F97\u4417\uC4A7\u7E3D\u645D\u1973"
		+ "\u6081\u4FDC\u222A\u9088\u46EE\uB814\uDE5E\u0BDB"
		+ "\uE032\u3A0A\u4906\u245C\uC2D3\uAC62\u9195\uE479"
		+ "\uE7C8\u376D\u8DD5\u4EA9\u6C56\uF4EA\u657A\uAE08"
		+ "\uBA78\u252E\u1CA6\uB4C6\uE8DD\u741F\u4BBD\u8B8A"
		+ "\u703E\uB566\u4803\uF60E\u6135\u57B9\u86C1\u1D9E"
		+ "\uE1F8\u9811\u69D9\u8E94\u9B1E\u87E9\uCE55\u28DF"
		+ "\u8CA1\u890D\uBFE6\u4268\u4199\u2D0F\uB054\uBB16";
	private final static int[] S = new int[256];
	private final static int[] SI = new int[256];
	private final static int[] T1 = new int[256];
	private final static int[] T2 = new int[256];
	private final static int[] T3 = new int[256];
	private final static int[] T4 = new int[256];
	private final static int[] T5 = new int[256];
	private final static int[] T6 = new int[256];
	private final static int[] T7 = new int[256];
	private final static int[] T8 = new int[256];
	private final static int[] U1 = new int[256];
	private final static int[] U2 = new int[256];
	private final static int[] U3 = new int[256];
	private final static int[] U4 = new int[256];
	private final static byte[] rcon = new byte[30];


	private static final Object _ignoreMeJvmDoesntRunInitializersSometimes = initializeStatics();
	private static Object initializeStatics()
	{
		int root = 0x11B;
		int i = 0;

		// S-box, inverse S-box, T-boxes, U-boxes
		int s, s2, s3, i2, i4, i8, i9, ib, id, ie, t;
		char c;
		for (i = 0; i < 256; i++)
		{
			c = SS.charAt(i >>> 1);
			S[i] = 255 & (((i & 1) == 0) ? c >>> 8 : c & 255);
			s = S[i] & 255;
			SI[s] = 255 & i;
			s2 = s << 1;
			if (s2 >= 0x100)
			{
				s2 ^= root;
			}
			s3 = s2 ^ s;
			i2 = i << 1;
			if (i2 >= 0x100)
			{
				i2 ^= root;
			}
			i4 = i2 << 1;
			if (i4 >= 0x100)
			{
				i4 ^= root;
			}
			i8 = i4 << 1;
			if (i8 >= 0x100)
			{
				i8 ^= root;
			}
			i9 = i8 ^ i;
			ib = i9 ^ i2;
			id = i9 ^ i4;
			ie = i8 ^ i4 ^ i2;

			T1[i] = t = (s2 << 24) | (s << 16) | (s << 8) | s3;
			T2[i] = (t >>> 8) | (t << 24);
			T3[i] = (t >>> 16) | (t << 16);
			T4[i] = (t >>> 24) | (t << 8);

			T5[s] = U1[i] = t = (ie << 24) | (i9 << 16) | (id << 8) | ib;
			T6[s] = U2[i] = (t >>> 8) | (t << 24);
			T7[s] = U3[i] = (t >>> 16) | (t << 16);
			T8[s] = U4[i] = (t >>> 24) | (t << 8);
		}
		//
		// round constants
		//
		int r = 1;
		rcon[0] = 1;
		for (i = 1; i < 30; i++)
		{
			r <<= 1;
			if (r >= 0x100)
			{
				r ^= root;
			}
			rcon[i] = (byte)r;
		}

		return null;
	}

	private transient int[][] ke;
	private transient int[][] kd;


	public AES()
	{
	}


	public AES(SecretKey aSecretKey)
	{
		engineInit(aSecretKey);
	}


	@Override
	public void engineInit(SecretKey aKey)
	{
		byte[] k = aKey.bytes();

		if (!(k.length == 16 || k.length == 24 || k.length == 32))
		{
			throw new IllegalArgumentException("Incorrect key length, expected either 16,24,32 bytes: " + k.length);
		}

		int bs = 16;

		int rounds = getRounds(k.length, bs);
		int bc = bs / 4;
		ke = new int[rounds + 1][bc]; // encryption round keys
		kd = new int[rounds + 1][bc]; // decryption round keys
		int roundKeyCount = (rounds + 1) * bc;
		int kc = k.length / 4;
		int[] tk = new int[kc];
		int i, j;

		// copy user material bytes into temporary ints
		for (i = 0, j = 0; i < kc;)
		{
			tk[i++] = k[j++] << 24
				| (k[j++] & 255) << 16
				| (k[j++] & 255) << 8
				| (k[j++] & 255);
		}
		// copy values into round key arrays
		int t = 0;
		for (j = 0; (j < kc) && (t < roundKeyCount); j++, t++)
		{
			ke[t / bc][t % bc] = tk[j];
			kd[rounds - (t / bc)][t % bc] = tk[j];
		}
		int tt, rconpointer = 0;
		while (t < roundKeyCount)
		{
			// extrapolate using phi (the round key evolution function)
			tt = tk[kc - 1];
			tk[0] ^= (S[(tt >>> 16) & 255] & 255) << 24
				^ (S[(tt >>> 8) & 255] & 255) << 16
				^ (S[tt & 255] & 255) << 8
				^ (S[(tt >>> 24)] & 255)
				^ rcon[rconpointer++] << 24;
			if (kc != 8)
			{
				for (i = 1, j = 0; i < kc;)
				{
					tk[i++] ^= tk[j++];
				}
			}
			else
			{
				for (i = 1, j = 0; i < kc / 2;)
				{
					tk[i++] ^= tk[j++];
				}
				tt = tk[kc / 2 - 1];
				tk[kc / 2] ^= (S[tt & 255] & 255)
					^ (S[(tt >>> 8) & 255] & 255) << 8
					^ (S[(tt >>> 16) & 255] & 255) << 16
					^ S[(tt >>> 24) & 255] << 24;
				for (j = kc / 2, i = j + 1; i < kc;)
				{
					tk[i++] ^= tk[j++];
				}
			}
			// copy values into round key arrays
			for (j = 0; (j < kc) && (t < roundKeyCount); j++, t++)
			{
				ke[t / bc][t % bc] = tk[j];
				kd[rounds - (t / bc)][t % bc] = tk[j];
			}
		}
		for (int r = 1; r < rounds; r++)
		{ // inverse MixColumn where needed
			for (j = 0; j < bc; j++)
			{
				tt = kd[r][j];
				kd[r][j] = U1[(tt >>> 24)]
					^ U2[(tt >>> 16) & 255]
					^ U3[(tt >>> 8) & 255]
					^ U4[tt & 255];
			}
		}
	}


	/**
	 * Encrypts a single block of plaintext in ECB-mode.
	 *
	 * @param in A buffer containing the plaintext to be encrypted.
	 * @param inOffset Index in the in buffer where plaintext should be read.
	 * @param out A buffer where ciphertext is written.
	 * @param outOffset Index in the out buffer where ciphertext should be written.
	 */
	@Override
	public void engineEncryptBlock(byte[] in, int inOffset, byte[] out, int outOffset)
	{
		int rounds = ke.length - 1;
		int[] ker = ke[0];

		int t0 = ((in[inOffset++] << 24) + ((in[inOffset++] & 255) << 16) + ((in[inOffset++] & 255) << 8) + (in[inOffset++] & 255)) ^ ker[0];
		int t1 = ((in[inOffset++] << 24) + ((in[inOffset++] & 255) << 16) + ((in[inOffset++] & 255) << 8) + (in[inOffset++] & 255)) ^ ker[1];
		int t2 = ((in[inOffset++] << 24) + ((in[inOffset++] & 255) << 16) + ((in[inOffset++] & 255) << 8) + (in[inOffset++] & 255)) ^ ker[2];
		int t3 = ((in[inOffset++] << 24) + ((in[inOffset++] & 255) << 16) + ((in[inOffset++] & 255) << 8) + (in[inOffset++] & 255)) ^ ker[3];

		for (int r = 1; r < rounds; r++)
		{
			ker = ke[r];
			int a0 = (T1[t0 >>> 24] ^ T2[(t1 >> 16) & 255] ^ T3[(t2 >> 8) & 255] ^ T4[t3 & 255]) ^ ker[0];
			int a1 = (T1[t1 >>> 24] ^ T2[(t2 >> 16) & 255] ^ T3[(t3 >> 8) & 255] ^ T4[t0 & 255]) ^ ker[1];
			int a2 = (T1[t2 >>> 24] ^ T2[(t3 >> 16) & 255] ^ T3[(t0 >> 8) & 255] ^ T4[t1 & 255]) ^ ker[2];
			int a3 = (T1[t3 >>> 24] ^ T2[(t0 >> 16) & 255] ^ T3[(t1 >> 8) & 255] ^ T4[t2 & 255]) ^ ker[3];
			t0 = a0;
			t1 = a1;
			t2 = a2;
			t3 = a3;
		}

		ker = ke[rounds];

		int tt0 = ker[0] ^ ((S[t0 >>> 24] << 24) + (S[(t1 >> 16) & 255] << 16) + (S[(t2 >> 8) & 255] << 8) + S[t3 & 255]);
		int tt1 = ker[1] ^ ((S[t1 >>> 24] << 24) + (S[(t2 >> 16) & 255] << 16) + (S[(t3 >> 8) & 255] << 8) + S[t0 & 255]);
		int tt2 = ker[2] ^ ((S[t2 >>> 24] << 24) + (S[(t3 >> 16) & 255] << 16) + (S[(t0 >> 8) & 255] << 8) + S[t1 & 255]);
		int tt3 = ker[3] ^ ((S[t3 >>> 24] << 24) + (S[(t0 >> 16) & 255] << 16) + (S[(t1 >> 8) & 255] << 8) + S[t2 & 255]);

		out[outOffset++] = (byte)(tt0 >>> 24);
		out[outOffset++] = (byte)(tt0 >> 16);
		out[outOffset++] = (byte)(tt0 >> 8);
		out[outOffset++] = (byte)(tt0);
		out[outOffset++] = (byte)(tt1 >>> 24);
		out[outOffset++] = (byte)(tt1 >> 16);
		out[outOffset++] = (byte)(tt1 >> 8);
		out[outOffset++] = (byte)(tt1);
		out[outOffset++] = (byte)(tt2 >>> 24);
		out[outOffset++] = (byte)(tt2 >> 16);
		out[outOffset++] = (byte)(tt2 >> 8);
		out[outOffset++] = (byte)(tt2);
		out[outOffset++] = (byte)(tt3 >>> 24);
		out[outOffset++] = (byte)(tt3 >> 16);
		out[outOffset++] = (byte)(tt3 >> 8);
		out[outOffset++] = (byte)(tt3);
	}


	/**
	 * Decrypts a single block of ciphertext in ECB-mode.
	 *
	 * @param in A buffer containing the ciphertext to be decrypted.
	 * @param inOffset Index in the in buffer where ciphertext should be read.
	 * @param out A buffer where plaintext is written.
	 * @param outOffset Index in the out buffer where plaintext should be written.
	 */
	@Override
	public void engineDecryptBlock(byte[] in, int inOffset, byte[] out, int outOffset)
	{
		int rounds = kd.length - 1;
		int[] kdr = kd[0];

		int t0 = ((in[inOffset++] << 24) + ((in[inOffset++] & 255) << 16) + ((in[inOffset++] & 255) << 8) + (in[inOffset++] & 255)) ^ kdr[0];
		int t1 = ((in[inOffset++] << 24) + ((in[inOffset++] & 255) << 16) + ((in[inOffset++] & 255) << 8) + (in[inOffset++] & 255)) ^ kdr[1];
		int t2 = ((in[inOffset++] << 24) + ((in[inOffset++] & 255) << 16) + ((in[inOffset++] & 255) << 8) + (in[inOffset++] & 255)) ^ kdr[2];
		int t3 = ((in[inOffset++] << 24) + ((in[inOffset++] & 255) << 16) + ((in[inOffset++] & 255) << 8) + (in[inOffset++] & 255)) ^ kdr[3];

		for (int r = 1; r < rounds; r++)
		{
			kdr = kd[r];
			int a0 = (T5[t0 >>> 24] ^ T6[(t3 >> 16) & 255] ^ T7[(t2 >> 8) & 255] ^ T8[t1 & 255]) ^ kdr[0];
			int a1 = (T5[t1 >>> 24] ^ T6[(t0 >> 16) & 255] ^ T7[(t3 >> 8) & 255] ^ T8[t2 & 255]) ^ kdr[1];
			int a2 = (T5[t2 >>> 24] ^ T6[(t1 >> 16) & 255] ^ T7[(t0 >> 8) & 255] ^ T8[t3 & 255]) ^ kdr[2];
			int a3 = (T5[t3 >>> 24] ^ T6[(t2 >> 16) & 255] ^ T7[(t1 >> 8) & 255] ^ T8[t0 & 255]) ^ kdr[3];
			t0 = a0;
			t1 = a1;
			t2 = a2;
			t3 = a3;
		}

		kdr = kd[rounds];

		int tt0 = kdr[0] ^ ((SI[t0 >>> 24] << 24) + (SI[(t3 >> 16) & 255] << 16) + (SI[(t2 >> 8) & 255] << 8) + SI[t1 & 255]);
		int tt1 = kdr[1] ^ ((SI[t1 >>> 24] << 24) + (SI[(t0 >> 16) & 255] << 16) + (SI[(t3 >> 8) & 255] << 8) + SI[t2 & 255]);
		int tt2 = kdr[2] ^ ((SI[t2 >>> 24] << 24) + (SI[(t1 >> 16) & 255] << 16) + (SI[(t0 >> 8) & 255] << 8) + SI[t3 & 255]);
		int tt3 = kdr[3] ^ ((SI[t3 >>> 24] << 24) + (SI[(t2 >> 16) & 255] << 16) + (SI[(t1 >> 8) & 255] << 8) + SI[t0 & 255]);

		out[outOffset++] = (byte)(tt0 >>> 24);
		out[outOffset++] = (byte)(tt0 >> 16);
		out[outOffset++] = (byte)(tt0 >> 8);
		out[outOffset++] = (byte)(tt0);
		out[outOffset++] = (byte)(tt1 >>> 24);
		out[outOffset++] = (byte)(tt1 >> 16);
		out[outOffset++] = (byte)(tt1 >> 8);
		out[outOffset++] = (byte)(tt1);
		out[outOffset++] = (byte)(tt2 >>> 24);
		out[outOffset++] = (byte)(tt2 >> 16);
		out[outOffset++] = (byte)(tt2 >> 8);
		out[outOffset++] = (byte)(tt2);
		out[outOffset++] = (byte)(tt3 >>> 24);
		out[outOffset++] = (byte)(tt3 >> 16);
		out[outOffset++] = (byte)(tt3 >> 8);
		out[outOffset++] = (byte)(tt3);
	}


	/**
	 * Encrypts a single block of plaintext in ECB-mode.
	 *
	 * @param in A buffer containing the plaintext to be encrypted.
	 * @param inOffset Index in the in buffer where plaintext should be read.
	 * @param out A buffer where ciphertext is written.
	 * @param outOffset Index in the out buffer where ciphertext should be written.
	 */
	@Override
	public void engineEncryptBlock(int[] in, int inOffset, int[] out, int outOffset)
	{
		int rounds = ke.length - 1;
		int[] ker = ke[0];

		// plaintext to ints + key
		int t0 = (in[inOffset + 0]) ^ ker[0];
		int t1 = (in[inOffset + 1]) ^ ker[1];
		int t2 = (in[inOffset + 2]) ^ ker[2];
		int t3 = (in[inOffset + 3]) ^ ker[3];

		for (int r = 1; r < rounds; r++)  // apply round transforms
		{
			ker = ke[r];
			int a0 = (T1[t0 >>> 24] ^ T2[(t1 >> 16) & 255] ^ T3[(t2 >> 8) & 255] ^ T4[t3 & 255]) ^ ker[0];
			int a1 = (T1[t1 >>> 24] ^ T2[(t2 >> 16) & 255] ^ T3[(t3 >> 8) & 255] ^ T4[t0 & 255]) ^ ker[1];
			int a2 = (T1[t2 >>> 24] ^ T2[(t3 >> 16) & 255] ^ T3[(t0 >> 8) & 255] ^ T4[t1 & 255]) ^ ker[2];
			int a3 = (T1[t3 >>> 24] ^ T2[(t0 >> 16) & 255] ^ T3[(t1 >> 8) & 255] ^ T4[t2 & 255]) ^ ker[3];
			t0 = a0;
			t1 = a1;
			t2 = a2;
			t3 = a3;
		}

		// last round is special
		ker = ke[rounds];

		out[outOffset + 0] = ker[0] ^ ((S[t0 >>> 24] << 24) + (S[(t1 >> 16) & 255] << 16) + (S[(t2 >> 8) & 255] << 8) + S[t3 & 255]);
		out[outOffset + 1] = ker[1] ^ ((S[t1 >>> 24] << 24) + (S[(t2 >> 16) & 255] << 16) + (S[(t3 >> 8) & 255] << 8) + S[t0 & 255]);
		out[outOffset + 2] = ker[2] ^ ((S[t2 >>> 24] << 24) + (S[(t3 >> 16) & 255] << 16) + (S[(t0 >> 8) & 255] << 8) + S[t1 & 255]);
		out[outOffset + 3] = ker[3] ^ ((S[t3 >>> 24] << 24) + (S[(t0 >> 16) & 255] << 16) + (S[(t1 >> 8) & 255] << 8) + S[t2 & 255]);
	}


	/**
	 * Decrypts a single block of ciphertext in ECB-mode.
	 *
	 * @param in A buffer containing the ciphertext to be decrypted.
	 * @param inOffset Index in the in buffer where ciphertext should be read.
	 * @param out A buffer where plaintext is written.
	 * @param outOffset Index in the out buffer where plaintext should be written.
	 */
	@Override
	public void engineDecryptBlock(int[] in, int inOffset, int[] out, int outOffset)
	{
		int rounds = kd.length - 1;
		int[] kdr = kd[0];

		// ciphertext to ints + key
		int t0 = (in[inOffset + 0]) ^ kdr[0];
		int t1 = (in[inOffset + 1]) ^ kdr[1];
		int t2 = (in[inOffset + 2]) ^ kdr[2];
		int t3 = (in[inOffset + 3]) ^ kdr[3];

		for (int r = 1; r < rounds; r++)  // apply round transforms
		{
			kdr = kd[r];
			int a0 = (T5[t0 >>> 24] ^ T6[(t3 >> 16) & 255] ^ T7[(t2 >> 8) & 255] ^ T8[t1 & 255]) ^ kdr[0];
			int a1 = (T5[t1 >>> 24] ^ T6[(t0 >> 16) & 255] ^ T7[(t3 >> 8) & 255] ^ T8[t2 & 255]) ^ kdr[1];
			int a2 = (T5[t2 >>> 24] ^ T6[(t1 >> 16) & 255] ^ T7[(t0 >> 8) & 255] ^ T8[t3 & 255]) ^ kdr[2];
			int a3 = (T5[t3 >>> 24] ^ T6[(t2 >> 16) & 255] ^ T7[(t1 >> 8) & 255] ^ T8[t0 & 255]) ^ kdr[3];
			t0 = a0;
			t1 = a1;
			t2 = a2;
			t3 = a3;
		}

		// last round is special
		kdr = kd[rounds];

		out[outOffset + 0] = (kdr[0] ^ (((SI[(t0 >>> 24)] << 24) + ((SI[(t3 >> 16) & 255] & 255) << 16) + ((SI[(t2 >> 8) & 255] & 255) << 8) + (SI[t1 & 255] & 255))));
		out[outOffset + 1] = (kdr[1] ^ (((SI[(t1 >>> 24)] << 24) + ((SI[(t0 >> 16) & 255] & 255) << 16) + ((SI[(t3 >> 8) & 255] & 255) << 8) + (SI[t2 & 255] & 255))));
		out[outOffset + 2] = (kdr[2] ^ (((SI[(t2 >>> 24)] << 24) + ((SI[(t1 >> 16) & 255] & 255) << 16) + ((SI[(t0 >> 8) & 255] & 255) << 8) + (SI[t3 & 255] & 255))));
		out[outOffset + 3] = (kdr[3] ^ (((SI[(t3 >>> 24)] << 24) + ((SI[(t2 >> 16) & 255] & 255) << 16) + ((SI[(t1 >> 8) & 255] & 255) << 8) + (SI[t0 & 255] & 255))));
	}


	private static int getRounds(int ks, int bs)
	{
		switch (ks)
		{
			case 16:
				return bs == 16 ? 10 : (bs == 24 ? 12 : 14);
			case 24:
				return bs != 32 ? 12 : 14;
			default: // 32 bytes = 256 bits
				return 14;
		}
	}


	/**
	 * Resets all internal state data. This Cipher object needs to be reinitialized again before it can be used again.
	 */
	@Override
	public void engineReset()
	{
		if (ke != null)
		{
			for (int i = 0; i < ke.length; i++)
			{
				Arrays.fill(ke[i], (byte)255);
				Arrays.fill(ke[i], (byte)0);
				Arrays.fill(kd[i], (byte)255);
				Arrays.fill(kd[i], (byte)0);
			}
		}
		ke = null;
		kd = null;
	}


	@Override
	public String toString()
	{
		return "AES";
	}
}

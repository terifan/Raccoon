package org.terifan.raccoon.security;

import java.util.Arrays;


/**
 * AES (Rijndael) is a variable block-size and variable key-size symmetric cipher.
 *
 * <p>References:</p>
 *
 * <ol>
 *	 <li><a href="http://www.esat.kuleuven.ac.be/~rijmen/rijndael/">The
 *	 Rijndael Block Cipher - AES Proposal</a>.<br>
 *	 <a href="mailto:vincent.rijmen@esat.kuleuven.ac.be">Vincent Rijmen</a> and
 *	 <a href="mailto:daemen.j@protonworld.com">Joan Daemen</a>.</li>
 * </ol>
 */
public final class AES extends Cipher
{
	private static final String SS =
			"\u637C\u777B\uF26B\u6FC5\u3001\u672B\uFED7\uAB76"
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
	private final static int[] Si = new int[256];
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


	static
	{
		int ROOT = 0x11B;
		int i, j = 0;

		// S-box, inverse S-box, T-boxes, U-boxes
		int s, s2, s3, i2, i4, i8, i9, ib, id, ie, t;
		char c;
		for (i = 0; i < 256; i++)
		{
			c = SS.charAt(i >>> 1);
			S[i] = 255 & (((i & 1) == 0) ? c >>> 8 : c & 255);
			s = S[i] & 255;
			Si[s] = 255 & i;
			s2 = s << 1;
			if (s2 >= 0x100)
			{
				s2 ^= ROOT;
			}
			s3 = s2 ^ s;
			i2 = i << 1;
			if (i2 >= 0x100)
			{
				i2 ^= ROOT;
			}
			i4 = i2 << 1;
			if (i4 >= 0x100)
			{
				i4 ^= ROOT;
			}
			i8 = i4 << 1;
			if (i8 >= 0x100)
			{
				i8 ^= ROOT;
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
				r ^= ROOT;
			}
			rcon[i] = (byte) r;
		}
	}
	private transient int[][] Ke;
	private transient int[][] Kd;
	private transient int mKeySize;


	public AES()
	{
		super();
	}


	public AES(SecretKey aSecretKey)
	{
		super(aSecretKey);
	}


	@Override
	public void engineInit(SecretKey aKey)
	{
		byte[] k = aKey.bytes();

		if (!(k.length == 16 || k.length == 24 || k.length == 32))
		{
			throw new IllegalArgumentException("Incorrect key length");
		}

		mKeySize = k.length;
		int bs = 16;

		int ROUNDS = getRounds(k.length, bs);
		int BC = bs / 4;
		Ke = new int[ROUNDS + 1][BC]; // encryption round keys
		Kd = new int[ROUNDS + 1][BC]; // decryption round keys
		int ROUND_KEY_COUNT = (ROUNDS + 1) * BC;
		int KC = k.length / 4;
		int[] tk = new int[KC];
		int i, j;

		// copy user material bytes into temporary ints
		for (i = 0, j = 0; i < KC;)
		{
			tk[i++] = k[j++] << 24
					| (k[j++] & 255) << 16
					| (k[j++] & 255) << 8
					| (k[j++] & 255);
		}
		// copy values into round key arrays
		int t = 0;
		for (j = 0; (j < KC) && (t < ROUND_KEY_COUNT); j++, t++)
		{
			Ke[t / BC][t % BC] = tk[j];
			Kd[ROUNDS - (t / BC)][t % BC] = tk[j];
		}
		int tt, rconpointer = 0;
		while (t < ROUND_KEY_COUNT)
		{
			// extrapolate using phi (the round key evolution function)
			tt = tk[KC - 1];
			tk[0] ^= (S[(tt >>> 16) & 255] & 255) << 24
					^ (S[(tt >>> 8) & 255] & 255) << 16
					^ (S[ tt & 255] & 255) << 8
					^ (S[(tt >>> 24)] & 255)
					^ rcon[rconpointer++] << 24;
			if (KC != 8)
			{
				for (i = 1, j = 0; i < KC;)
				{
					tk[i++] ^= tk[j++];
				}
			}
			else
			{
				for (i = 1, j = 0; i < KC / 2;)
				{
					tk[i++] ^= tk[j++];
				}
				tt = tk[KC / 2 - 1];
				tk[KC / 2] ^= (S[ tt & 255] & 255)
						^ (S[(tt >>> 8) & 255] & 255) << 8
						^ (S[(tt >>> 16) & 255] & 255) << 16
						^ S[(tt >>> 24) & 255] << 24;
				for (j = KC / 2, i = j + 1; i < KC;)
				{
					tk[i++] ^= tk[j++];
				}
			}
			// copy values into round key arrays
			for (j = 0; (j < KC) && (t < ROUND_KEY_COUNT); j++, t++)
			{
				Ke[t / BC][t % BC] = tk[j];
				Kd[ROUNDS - (t / BC)][t % BC] = tk[j];
			}
		}
		for (int r = 1; r < ROUNDS; r++)
		{ // inverse MixColumn where needed
			for (j = 0; j < BC; j++)
			{
				tt = Kd[r][j];
				Kd[r][j] = U1[(tt >>> 24)]
						^ U2[(tt >>> 16) & 255]
						^ U3[(tt >>> 8) & 255]
						^ U4[ tt & 255];
			}
		}
	}


	/**
	 * Encrypts a single block of plaintext in ECB-mode.
	 *
	 * @param in
	 *    A buffer containing the plaintext to be encrypted.
	 * @param inOffset
	 *    Index in the in buffer where plaintext should be read.
	 * @param out
	 *    A buffer where ciphertext is written.
	 * @param outOffset
	 *    Index in the out buffer where ciphertext should be written.
	 */
//	@Override
//	public void engineEncryptBlock(byte[] in, int inOffset, byte[] out, int outOffset)
//	{
//		int ROUNDS = Ke.length - 1;
//		int[] Ker = Ke[0];
//		int[] _T1 = T1;
//		int[] _T2 = T2;
//		int[] _T3 = T3;
//		int[] _T4 = T4;
//
//		int t0 = ((in[inOffset++] << 24) + ((in[inOffset++] & 255) << 16) + ((in[inOffset++] & 255) << 8) + (in[inOffset++] & 255)) ^ Ker[0];
//		int t1 = ((in[inOffset++] << 24) + ((in[inOffset++] & 255) << 16) + ((in[inOffset++] & 255) << 8) + (in[inOffset++] & 255)) ^ Ker[1];
//		int t2 = ((in[inOffset++] << 24) + ((in[inOffset++] & 255) << 16) + ((in[inOffset++] & 255) << 8) + (in[inOffset++] & 255)) ^ Ker[2];
//		int t3 = ((in[inOffset++] << 24) + ((in[inOffset++] & 255) << 16) + ((in[inOffset++] & 255) << 8) + (in[inOffset++] & 255)) ^ Ker[3];
//
//		for (int r = 1; r < ROUNDS; r++)
//		{
//			Ker = Ke[r];
//			int a0 = (_T1[t0 >>> 24] ^ _T2[(t1 >> 16) & 255] ^ _T3[(t2 >> 8) & 255] ^ _T4[t3 & 255]) ^ Ker[0];
//			int a1 = (_T1[t1 >>> 24] ^ _T2[(t2 >> 16) & 255] ^ _T3[(t3 >> 8) & 255] ^ _T4[t0 & 255]) ^ Ker[1];
//			int a2 = (_T1[t2 >>> 24] ^ _T2[(t3 >> 16) & 255] ^ _T3[(t0 >> 8) & 255] ^ _T4[t1 & 255]) ^ Ker[2];
//			int a3 = (_T1[t3 >>> 24] ^ _T2[(t0 >> 16) & 255] ^ _T3[(t1 >> 8) & 255] ^ _T4[t2 & 255]) ^ Ker[3];
//			t0 = a0;
//			t1 = a1;
//			t2 = a2;
//			t3 = a3;
//		}
//
//		Ker = Ke[ROUNDS];
//		int[] _S = S;
//
//		int tt0 = Ker[0] ^ ((_S[t0 >>> 24] << 24) + (_S[(t1 >> 16) & 255] << 16) + (_S[(t2 >> 8) & 255] << 8) + _S[t3 & 255]);
//		int tt1 = Ker[1] ^ ((_S[t1 >>> 24] << 24) + (_S[(t2 >> 16) & 255] << 16) + (_S[(t3 >> 8) & 255] << 8) + _S[t0 & 255]);
//		int tt2 = Ker[2] ^ ((_S[t2 >>> 24] << 24) + (_S[(t3 >> 16) & 255] << 16) + (_S[(t0 >> 8) & 255] << 8) + _S[t1 & 255]);
//		int tt3 = Ker[3] ^ ((_S[t3 >>> 24] << 24) + (_S[(t0 >> 16) & 255] << 16) + (_S[(t1 >> 8) & 255] << 8) + _S[t2 & 255]);
//
//		out[outOffset++] = (byte) (tt0 >>> 24);
//		out[outOffset++] = (byte) (tt0 >> 16);
//		out[outOffset++] = (byte) (tt0 >> 8);
//		out[outOffset++] = (byte) (tt0);
//		out[outOffset++] = (byte) (tt1 >>> 24);
//		out[outOffset++] = (byte) (tt1 >> 16);
//		out[outOffset++] = (byte) (tt1 >> 8);
//		out[outOffset++] = (byte) (tt1);
//		out[outOffset++] = (byte) (tt2 >>> 24);
//		out[outOffset++] = (byte) (tt2 >> 16);
//		out[outOffset++] = (byte) (tt2 >> 8);
//		out[outOffset++] = (byte) (tt2);
//		out[outOffset++] = (byte) (tt3 >>> 24);
//		out[outOffset++] = (byte) (tt3 >> 16);
//		out[outOffset++] = (byte) (tt3 >> 8);
//		out[outOffset++] = (byte) (tt3);
//	}
//
//
//	/**
//	 * Decrypts a single block of ciphertext in ECB-mode.
//	 *
//	 * @param in
//	 *    A buffer containing the ciphertext to be decrypted.
//	 * @param inOffset
//	 *    Index in the in buffer where ciphertext should be read.
//	 * @param out
//	 *    A buffer where plaintext is written.
//	 * @param outOffset
//	 *    Index in the out buffer where plaintext should be written.
//	 */
//	@Override
//	public void engineDecryptBlock(byte[] in, int inOffset, byte[] out, int outOffset)
//	{
//		int ROUNDS = Kd.length - 1;
//		int[] Kdr = Kd[0];
//
//		int t0 = ((in[inOffset++] << 24) + ((in[inOffset++] & 255) << 16) + ((in[inOffset++] & 255) << 8) + (in[inOffset++] & 255)) ^ Kdr[0];
//		int t1 = ((in[inOffset++] << 24) + ((in[inOffset++] & 255) << 16) + ((in[inOffset++] & 255) << 8) + (in[inOffset++] & 255)) ^ Kdr[1];
//		int t2 = ((in[inOffset++] << 24) + ((in[inOffset++] & 255) << 16) + ((in[inOffset++] & 255) << 8) + (in[inOffset++] & 255)) ^ Kdr[2];
//		int t3 = ((in[inOffset++] << 24) + ((in[inOffset++] & 255) << 16) + ((in[inOffset++] & 255) << 8) + (in[inOffset++] & 255)) ^ Kdr[3];
//
//		for (int r = 1; r < ROUNDS; r++)
//		{
//			Kdr = Kd[r];
//			int a0 = (T5[t0 >>> 24] ^ T6[(t3 >> 16) & 255] ^ T7[(t2 >> 8) & 255] ^ T8[t1 & 255]) ^ Kdr[0];
//			int a1 = (T5[t1 >>> 24] ^ T6[(t0 >> 16) & 255] ^ T7[(t3 >> 8) & 255] ^ T8[t2 & 255]) ^ Kdr[1];
//			int a2 = (T5[t2 >>> 24] ^ T6[(t1 >> 16) & 255] ^ T7[(t0 >> 8) & 255] ^ T8[t3 & 255]) ^ Kdr[2];
//			int a3 = (T5[t3 >>> 24] ^ T6[(t2 >> 16) & 255] ^ T7[(t1 >> 8) & 255] ^ T8[t0 & 255]) ^ Kdr[3];
//			t0 = a0;
//			t1 = a1;
//			t2 = a2;
//			t3 = a3;
//		}
//
//		Kdr = Kd[ROUNDS];
//		int[] _S = Si;
//
//		int tt0 = Kdr[0] ^ ((_S[t0 >>> 24] << 24) + (_S[(t3 >> 16) & 255] << 16) + (_S[(t2 >> 8) & 255] << 8) + _S[t1 & 255]);
//		int tt1 = Kdr[1] ^ ((_S[t1 >>> 24] << 24) + (_S[(t0 >> 16) & 255] << 16) + (_S[(t3 >> 8) & 255] << 8) + _S[t2 & 255]);
//		int tt2 = Kdr[2] ^ ((_S[t2 >>> 24] << 24) + (_S[(t1 >> 16) & 255] << 16) + (_S[(t0 >> 8) & 255] << 8) + _S[t3 & 255]);
//		int tt3 = Kdr[3] ^ ((_S[t3 >>> 24] << 24) + (_S[(t2 >> 16) & 255] << 16) + (_S[(t1 >> 8) & 255] << 8) + _S[t0 & 255]);
//
//		out[outOffset++] = (byte) (tt0 >>> 24);
//		out[outOffset++] = (byte) (tt0 >> 16);
//		out[outOffset++] = (byte) (tt0 >> 8);
//		out[outOffset++] = (byte) (tt0);
//		out[outOffset++] = (byte) (tt1 >>> 24);
//		out[outOffset++] = (byte) (tt1 >> 16);
//		out[outOffset++] = (byte) (tt1 >> 8);
//		out[outOffset++] = (byte) (tt1);
//		out[outOffset++] = (byte) (tt2 >>> 24);
//		out[outOffset++] = (byte) (tt2 >> 16);
//		out[outOffset++] = (byte) (tt2 >> 8);
//		out[outOffset++] = (byte) (tt2);
//		out[outOffset++] = (byte) (tt3 >>> 24);
//		out[outOffset++] = (byte) (tt3 >> 16);
//		out[outOffset++] = (byte) (tt3 >> 8);
//		out[outOffset++] = (byte) (tt3);
//	}


	/**
	 * Encrypts a single block of plaintext in ECB-mode.
	 *
	 * @param in
	 *    A buffer containing the plaintext to be encrypted.
	 * @param inOffset
	 *    Index in the in buffer where plaintext should be read.
	 * @param out
	 *    A buffer where ciphertext is written.
	 * @param outOffset
	 *    Index in the out buffer where ciphertext should be written.
	 */
	@Override
	public void engineEncryptBlock(int[] in, int inOffset, int[] out, int outOffset)
	{
		int ROUNDS = Ke.length - 1;
		int[] Ker = Ke[0];

		// plaintext to ints + key
		int t0 = (in[inOffset + 0]) ^ Ker[0];
		int t1 = (in[inOffset + 1]) ^ Ker[1];
		int t2 = (in[inOffset + 2]) ^ Ker[2];
		int t3 = (in[inOffset + 3]) ^ Ker[3];

		int[] _T1 = T1;
		int[] _T2 = T2;
		int[] _T3 = T3;
		int[] _T4 = T4;

		for (int r = 1; r < ROUNDS; r++)  // apply round transforms
		{
			Ker = Ke[r];
			int a0 = (_T1[t0 >>> 24] ^ _T2[(t1 >> 16) & 255] ^ _T3[(t2 >> 8) & 255] ^ _T4[t3 & 255]) ^ Ker[0];
			int a1 = (_T1[t1 >>> 24] ^ _T2[(t2 >> 16) & 255] ^ _T3[(t3 >> 8) & 255] ^ _T4[t0 & 255]) ^ Ker[1];
			int a2 = (_T1[t2 >>> 24] ^ _T2[(t3 >> 16) & 255] ^ _T3[(t0 >> 8) & 255] ^ _T4[t1 & 255]) ^ Ker[2];
			int a3 = (_T1[t3 >>> 24] ^ _T2[(t0 >> 16) & 255] ^ _T3[(t1 >> 8) & 255] ^ _T4[t2 & 255]) ^ Ker[3];
			t0 = a0;
			t1 = a1;
			t2 = a2;
			t3 = a3;
		}

		// last round is special
		Ker = Ke[ROUNDS];
		int[] _S = S;

		out[outOffset + 0] = Ker[0] ^ ((_S[t0 >>> 24] << 24) + (_S[(t1 >> 16) & 255] << 16) + (_S[(t2 >> 8) & 255] << 8) + _S[t3 & 255]);
		out[outOffset + 1] = Ker[1] ^ ((_S[t1 >>> 24] << 24) + (_S[(t2 >> 16) & 255] << 16) + (_S[(t3 >> 8) & 255] << 8) + _S[t0 & 255]);
		out[outOffset + 2] = Ker[2] ^ ((_S[t2 >>> 24] << 24) + (_S[(t3 >> 16) & 255] << 16) + (_S[(t0 >> 8) & 255] << 8) + _S[t1 & 255]);
		out[outOffset + 3] = Ker[3] ^ ((_S[t3 >>> 24] << 24) + (_S[(t0 >> 16) & 255] << 16) + (_S[(t1 >> 8) & 255] << 8) + _S[t2 & 255]);
	}


	/**
	 * Decrypts a single block of ciphertext in ECB-mode.
	 *
	 * @param in
	 *    A buffer containing the ciphertext to be decrypted.
	 * @param inOffset
	 *    Index in the in buffer where ciphertext should be read.
	 * @param out
	 *    A buffer where plaintext is written.
	 * @param outOffset
	 *    Index in the out buffer where plaintext should be written.
	 */
	@Override
	public void engineDecryptBlock(int[] in, int inOffset, int[] out, int outOffset)
	{
		int ROUNDS = Kd.length - 1;
		int[] Kdr = Kd[0];

		// ciphertext to ints + key
		int t0 = (in[inOffset + 0]) ^ Kdr[0];
		int t1 = (in[inOffset + 1]) ^ Kdr[1];
		int t2 = (in[inOffset + 2]) ^ Kdr[2];
		int t3 = (in[inOffset + 3]) ^ Kdr[3];

		int[] _T5 = T5;
		int[] _T6 = T6;
		int[] _T7 = T7;
		int[] _T8 = T8;

		for (int r = 1; r < ROUNDS; r++)  // apply round transforms
		{
			Kdr = Kd[r];
			int a0 = (_T5[t0 >>> 24] ^ _T6[(t3 >> 16) & 255] ^ _T7[(t2 >> 8) & 255] ^ _T8[t1 & 255]) ^ Kdr[0];
			int a1 = (_T5[t1 >>> 24] ^ _T6[(t0 >> 16) & 255] ^ _T7[(t3 >> 8) & 255] ^ _T8[t2 & 255]) ^ Kdr[1];
			int a2 = (_T5[t2 >>> 24] ^ _T6[(t1 >> 16) & 255] ^ _T7[(t0 >> 8) & 255] ^ _T8[t3 & 255]) ^ Kdr[2];
			int a3 = (_T5[t3 >>> 24] ^ _T6[(t2 >> 16) & 255] ^ _T7[(t1 >> 8) & 255] ^ _T8[t0 & 255]) ^ Kdr[3];
			t0 = a0;
			t1 = a1;
			t2 = a2;
			t3 = a3;
		}

		// last round is special
		Kdr = Kd[ROUNDS];
		int[] _Si = Si;

		out[outOffset + 0] = (Kdr[0] ^ (((_Si[(t0 >>> 24)] << 24) + ((_Si[(t3 >> 16) & 255] & 255) << 16) + ((_Si[(t2 >> 8) & 255] & 255) << 8) + (_Si[t1 & 255] & 255))));
		out[outOffset + 1] = (Kdr[1] ^ (((_Si[(t1 >>> 24)] << 24) + ((_Si[(t0 >> 16) & 255] & 255) << 16) + ((_Si[(t3 >> 8) & 255] & 255) << 8) + (_Si[t2 & 255] & 255))));
		out[outOffset + 2] = (Kdr[2] ^ (((_Si[(t2 >>> 24)] << 24) + ((_Si[(t1 >> 16) & 255] & 255) << 16) + ((_Si[(t0 >> 8) & 255] & 255) << 8) + (_Si[t3 & 255] & 255))));
		out[outOffset + 3] = (Kdr[3] ^ (((_Si[(t3 >>> 24)] << 24) + ((_Si[(t2 >> 16) & 255] & 255) << 16) + ((_Si[(t1 >> 8) & 255] & 255) << 8) + (_Si[t0 & 255] & 255))));
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
	 * Returns the block size.
	 */
	@Override
	public int engineGetBlockSize()
	{
		return 16;
	}


	/**
	 * Returns the key size.
	 */
	@Override
	public int engineGetKeySize()
	{
		return mKeySize;
	}


	/**
	 * Resets all internal state data. This Cipher object needs to be
	 * reinitialized again before it can be used again.
	 */
	@Override
	public void engineReset()
	{
		mKeySize = 0;
		if (Ke != null)
		{
			for (int i = 0; i < Ke.length; i++)
			{
				Arrays.fill(Ke[i], (byte) 255);
				Arrays.fill(Ke[i], (byte) 0);
				Arrays.fill(Kd[i], (byte) 255);
				Arrays.fill(Kd[i], (byte) 0);
			}
		}
		Ke = null;
		Kd = null;
	}


	@Override
	public String toString()
	{
		return "AES";
	}


	/**
	 * This method returns a new instance of the Twofish cipher.
	 */
	@Override
	public Cipher newInstance()
	{
		return new AES();
	}
}
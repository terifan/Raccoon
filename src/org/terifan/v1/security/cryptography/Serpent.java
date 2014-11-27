package org.terifan.v1.security.cryptography;


/**
 * Serpent is a 128-bit 32-round block cipher with variable key lengths,
 * including 128-, 192- and 256-bit keys conjectured to be at least as secure
 * as three-key triple-DES.<p>
 *
 * Serpent was designed by Ross Anderson, Eli Biham and Lars Knudsen as a
 * candidate algorithm for the NIST AES Quest.<p>
 *
 * References:<ol>
 * <li>Serpent: A New Block Cipher Proposal. This paper was published in the
 * proceedings of the "Fast Software Encryption Workshop No. 5" held in
 * Paris in March 1998. LNCS, Springer Verlag.<p>
 * <li>Reference implementation of the standard Serpent cipher written in C
 * by <a href="http://www.cl.cam.ac.uk/~fms/"> Frank Stajano</a>.</ol><p>
 *
 * <b>Copyright</b> &copy; 1997, 1998
 * <a href="http://www.systemics.com/">Systemics Ltd</a> on behalf of the
 * <a href="http://www.systemics.com/docs/cryptix/">Cryptix Development Team</a>.
 * <br>All rights reserved.<p>
 *
 * @author Raif S. Naffah
 * @author Serpent authors (Ross Anderson, Eli Biham and Lars Knudsen)
 */
public class Serpent extends Cipher
{
	private static final int DEFAULT_KEY_SIZE = 16;
	private static final int DEFAULT_BLOCK_SIZE = 16;
	private static final int ROUNDS = 32;

	// The fractional part of the golden ratio, (sqrt(5)+1)/2.
	private static final int PHI = 0x9e3779b9;

	private transient int mKeySize;

	private transient int x0, x1, x2, x3, x4;

	private transient int k0, k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, k12, k13,
	                      k14, k15, k16, k17, k18, k19, k20, k21, k22, k23, k24, k25, k26,
	                      k27, k28, k29, k30, k31, k32, k33, k34, k35, k36, k37, k38, k39,
	                      k40, k41, k42, k43, k44, k45, k46, k47, k48, k49, k50, k51, k52,
	                      k53, k54, k55, k56, k57, k58, k59, k60, k61, k62, k63, k64, k65,
	                      k66, k67, k68, k69, k70, k71, k72, k73, k74, k75, k76, k77, k78,
	                      k79, k80, k81, k82, k83, k84, k85, k86, k87, k88, k89, k90, k91,
	                      k92, k93, k94, k95, k96, k97, k98, k99, k100, k101, k102, k103,
	                      k104, k105, k106, k107, k108, k109, k110, k111, k112, k113, k114,
	                      k115, k116, k117, k118, k119, k120, k121, k122, k123, k124, k125,
	                      k126, k127, k128, k129, k130, k131;


	public Serpent()
	{
		super();
	}


	public Serpent(SecretKey aKey)
	{
		super(aKey);
	}


	@Override
	public void engineInit(SecretKey aKey)
	{
		byte [] kb = aKey.bytes();

		if (kb == null || (kb.length != 16 && kb.length != 24 && kb.length != 32))
		{
			throw new IllegalArgumentException("Invalid k: " + (kb==null?"null":kb.length+" bytes != {16,24,32}"));
		}

		mKeySize = kb.length;

		// Here w is our "pre-key".
		int[] w = new int[4 * (ROUNDS + 1)];
		int i, j;
		for (i = 0, j = 0; i < 8 && j < kb.length; i++)
		{
			w[i] = (kb[j++] & 0xff)		 | (kb[j++] & 0xff) <<  8 |
					 (kb[j++] & 0xff) << 16 | (kb[j++] & 0xff) << 24;
		}
		// Pad key if < 256 bits.
		if (i != 8)
		{
			w[i] = 1;
		}
		// Transform using w_i-8 ... w_i-1
		for (i = 8, j = 0; i < 16; i++)
		{
			int t = w[j] ^ w[i - 5] ^ w[i - 3] ^ w[i - 1] ^ PHI ^ j++;
			w[i] = t << 11 | t >>> 21;
		}
		// Translate by 8.
		for (i = 0; i < 8; i++)
		{
			w[i] = w[i + 8];
		}
		// Transform the rest of the key.
		for ( ; i < w.length; i++)
		{
			int t = w[i - 8] ^ w[i - 5] ^ w[i - 3] ^ w[i - 1] ^ PHI ^ i;
			w[i] = t << 11 | t >>> 21;
		}

		// After these s-boxes the pre-key (w, above) will become the
		// session key (key, below).
		sbox3(w[0], w[1], w[2], w[3]);
		k0 = x0; k1 = x1; k2 = x2; k3 = x3;
		sbox2(w[4], w[5], w[6], w[7]);
		k4 = x0; k5 = x1; k6 = x2; k7 = x3;
		sbox1(w[8], w[9], w[10], w[11]);
		k8 = x0; k9 = x1; k10 = x2; k11 = x3;
		sbox0(w[12], w[13], w[14], w[15]);
		k12 = x0; k13 = x1; k14 = x2; k15 = x3;
		sbox7(w[16], w[17], w[18], w[19]);
		k16 = x0; k17 = x1; k18 = x2; k19 = x3;
		sbox6(w[20], w[21], w[22], w[23]);
		k20 = x0; k21 = x1; k22 = x2; k23 = x3;
		sbox5(w[24], w[25], w[26], w[27]);
		k24 = x0; k25 = x1; k26 = x2; k27 = x3;
		sbox4(w[28], w[29], w[30], w[31]);
		k28 = x0; k29 = x1; k30 = x2; k31 = x3;
		sbox3(w[32], w[33], w[34], w[35]);
		k32 = x0; k33 = x1; k34 = x2; k35 = x3;
		sbox2(w[36], w[37], w[38], w[39]);
		k36 = x0; k37 = x1; k38 = x2; k39 = x3;
		sbox1(w[40], w[41], w[42], w[43]);
		k40 = x0; k41 = x1; k42 = x2; k43 = x3;
		sbox0(w[44], w[45], w[46], w[47]);
		k44 = x0; k45 = x1; k46 = x2; k47 = x3;
		sbox7(w[48], w[49], w[50], w[51]);
		k48 = x0; k49 = x1; k50 = x2; k51 = x3;
		sbox6(w[52], w[53], w[54], w[55]);
		k52 = x0; k53 = x1; k54 = x2; k55 = x3;
		sbox5(w[56], w[57], w[58], w[59]);
		k56 = x0; k57 = x1; k58 = x2; k59 = x3;
		sbox4(w[60], w[61], w[62], w[63]);
		k60 = x0; k61 = x1; k62 = x2; k63 = x3;
		sbox3(w[64], w[65], w[66], w[67]);
		k64 = x0; k65 = x1; k66 = x2; k67 = x3;
		sbox2(w[68], w[69], w[70], w[71]);
		k68 = x0; k69 = x1; k70 = x2; k71 = x3;
		sbox1(w[72], w[73], w[74], w[75]);
		k72 = x0; k73 = x1; k74 = x2; k75 = x3;
		sbox0(w[76], w[77], w[78], w[79]);
		k76 = x0; k77 = x1; k78 = x2; k79 = x3;
		sbox7(w[80], w[81], w[82], w[83]);
		k80 = x0; k81 = x1; k82 = x2; k83 = x3;
		sbox6(w[84], w[85], w[86], w[87]);
		k84 = x0; k85 = x1; k86 = x2; k87 = x3;
		sbox5(w[88], w[89], w[90], w[91]);
		k88 = x0; k89 = x1; k90 = x2; k91 = x3;
		sbox4(w[92], w[93], w[94], w[95]);
		k92 = x0; k93 = x1; k94 = x2; k95 = x3;
		sbox3(w[96], w[97], w[98], w[99]);
		k96 = x0; k97 = x1; k98 = x2; k99 = x3;
		sbox2(w[100], w[101], w[102], w[103]);
		k100 = x0; k101 = x1; k102 = x2; k103 = x3;
		sbox1(w[104], w[105], w[106], w[107]);
		k104 = x0; k105 = x1; k106 = x2; k107 = x3;
		sbox0(w[108], w[109], w[110], w[111]);
		k108 = x0; k109 = x1; k110 = x2; k111 = x3;
		sbox7(w[112], w[113], w[114], w[115]);
		k112 = x0; k113 = x1; k114 = x2; k115 = x3;
		sbox6(w[116], w[117], w[118], w[119]);
		k116 = x0; k117 = x1; k118 = x2; k119 = x3;
		sbox5(w[120], w[121], w[122], w[123]);
		k120 = x0; k121 = x1; k122 = x2; k123 = x3;
		sbox4(w[124], w[125], w[126], w[127]);
		k124 = x0; k125 = x1; k126 = x2; k127 = x3;
		sbox3(w[128], w[129], w[130], w[131]);
		k128 = x0; k129 = x1; k130 = x2; k131 = x3;
	}


	/**
	 * Encrypts a single block of ciphertext in ECB-mode.
	 *
	 * @param in
	 *	 A buffer containing the plaintext to be encrypted.
	 * @param inOffset
	 *	 Index in the in buffer where plaintext should be read.
	 * @param out
	 *	 A buffer where ciphertext is written.
	 * @param outOffset
	 *	 Index in the out buffer where ciphertext should be written.
	 */
	@Override
	public void engineEncryptBlock(byte [] in, int inOffset, byte [] out, int outOffset)
	{
		x0 = (in[inOffset	] & 0xff) | (in[inOffset+ 1] & 0xff) <<  8 | (in[inOffset+ 2] & 0xff) << 16 | (in[inOffset+ 3] & 0xff) << 24;
		x1 = (in[inOffset+ 4] & 0xff) | (in[inOffset+ 5] & 0xff) <<  8 | (in[inOffset+ 6] & 0xff) << 16 | (in[inOffset+ 7] & 0xff) << 24;
		x2 = (in[inOffset+ 8] & 0xff) | (in[inOffset+ 9] & 0xff) <<  8 | (in[inOffset+10] & 0xff) << 16 | (in[inOffset+11] & 0xff) << 24;
		x3 = (in[inOffset+12] & 0xff) | (in[inOffset+13] & 0xff) <<  8 | (in[inOffset+14] & 0xff) << 16 | (in[inOffset+15] & 0xff) << 24;

		x0 ^= k0; x1 ^= k1; x2 ^= k2; x3 ^= k3; sbox0();
		x1 ^= k4; x4 ^= k5; x2 ^= k6; x0 ^= k7; sbox1();
		x0 ^= k8; x4 ^= k9; x2 ^= k10; x1 ^= k11; sbox2();
		x2 ^= k12; x1 ^= k13; x4 ^= k14; x3 ^= k15; sbox3();
		x1 ^= k16; x4 ^= k17; x3 ^= k18; x0 ^= k19; sbox4();
		x4 ^= k20; x2 ^= k21; x1 ^= k22; x0 ^= k23; sbox5();
		x2 ^= k24; x0 ^= k25; x4 ^= k26; x1 ^= k27; sbox6();
		x2 ^= k28; x0 ^= k29; x3 ^= k30; x4 ^= k31; sbox7();
		x0 = x3; x3 = x2; x2 = x4;

		x0 ^= k32; x1 ^= k33; x2 ^= k34; x3 ^= k35; sbox0();
		x1 ^= k36; x4 ^= k37; x2 ^= k38; x0 ^= k39; sbox1();
		x0 ^= k40; x4 ^= k41; x2 ^= k42; x1 ^= k43; sbox2();
		x2 ^= k44; x1 ^= k45; x4 ^= k46; x3 ^= k47; sbox3();
		x1 ^= k48; x4 ^= k49; x3 ^= k50; x0 ^= k51; sbox4();
		x4 ^= k52; x2 ^= k53; x1 ^= k54; x0 ^= k55; sbox5();
		x2 ^= k56; x0 ^= k57; x4 ^= k58; x1 ^= k59; sbox6();
		x2 ^= k60; x0 ^= k61; x3 ^= k62; x4 ^= k63; sbox7();
		x0 = x3; x3 = x2; x2 = x4;

		x0 ^= k64; x1 ^= k65; x2 ^= k66; x3 ^= k67; sbox0();
		x1 ^= k68; x4 ^= k69; x2 ^= k70; x0 ^= k71; sbox1();
		x0 ^= k72; x4 ^= k73; x2 ^= k74; x1 ^= k75; sbox2();
		x2 ^= k76; x1 ^= k77; x4 ^= k78; x3 ^= k79; sbox3();
		x1 ^= k80; x4 ^= k81; x3 ^= k82; x0 ^= k83; sbox4();
		x4 ^= k84; x2 ^= k85; x1 ^= k86; x0 ^= k87; sbox5();
		x2 ^= k88; x0 ^= k89; x4 ^= k90; x1 ^= k91; sbox6();
		x2 ^= k92; x0 ^= k93; x3 ^= k94; x4 ^= k95; sbox7();
		x0 = x3; x3 = x2; x2 = x4;

		x0 ^= k96; x1 ^= k97; x2 ^= k98; x3 ^= k99; sbox0();
		x1 ^= k100; x4 ^= k101; x2 ^= k102; x0 ^= k103; sbox1();
		x0 ^= k104; x4 ^= k105; x2 ^= k106; x1 ^= k107; sbox2();
		x2 ^= k108; x1 ^= k109; x4 ^= k110; x3 ^= k111; sbox3();
		x1 ^= k112; x4 ^= k113; x3 ^= k114; x0 ^= k115; sbox4();
		x4 ^= k116; x2 ^= k117; x1 ^= k118; x0 ^= k119; sbox5();
		x2 ^= k120; x0 ^= k121; x4 ^= k122; x1 ^= k123; sbox6();
		x2 ^= k124; x0 ^= k125; x3 ^= k126; x4 ^= k127; sbox7noLT();
		x0 = x3; x3 = x2; x2 = x4;
		x0 ^= k128; x1 ^= k129; x2 ^= k130; x3 ^= k131;

		out[outOffset	] = (byte) x0;
		out[outOffset+ 1] = (byte)(x0 >>>  8);
		out[outOffset+ 2] = (byte)(x0 >>> 16);
		out[outOffset+ 3] = (byte)(x0 >>> 24);
		out[outOffset+ 4] = (byte) x1;
		out[outOffset+ 5] = (byte)(x1 >>>  8);
		out[outOffset+ 6] = (byte)(x1 >>> 16);
		out[outOffset+ 7] = (byte)(x1 >>> 24);
		out[outOffset+ 8] = (byte) x2;
		out[outOffset+ 9] = (byte)(x2 >>>  8);
		out[outOffset+10] = (byte)(x2 >>> 16);
		out[outOffset+11] = (byte)(x2 >>> 24);
		out[outOffset+12] = (byte) x3;
		out[outOffset+13] = (byte)(x3 >>>  8);
		out[outOffset+14] = (byte)(x3 >>> 16);
		out[outOffset+15] = (byte)(x3 >>> 24);
	}


	/**
	 * Decrypts a single block of ciphertext in ECB-mode.
	 *
	 * @param in
	 *   A buffer containing the ciphertext to be decrypted.
	 * @param inOffset
	 *   Index in the in buffer where ciphertext should be read.
	 * @param out
	 *   A buffer where plaintext is written.
	 * @param outOffset
	 *   Index in the out buffer where plaintext should be written.
	 */
	@Override
	public void engineDecryptBlock(byte [] in, int inOffset, byte [] out, int outOffset)
	{
		x0 = (in[inOffset	] & 0xff) | (in[inOffset+ 1] & 0xff) <<  8 | (in[inOffset+ 2] & 0xff) << 16 | (in[inOffset+ 3] & 0xff) << 24;
		x1 = (in[inOffset+ 4] & 0xff) | (in[inOffset+ 5] & 0xff) <<  8 | (in[inOffset+ 6] & 0xff) << 16 | (in[inOffset+ 7] & 0xff) << 24;
		x2 = (in[inOffset+ 8] & 0xff) | (in[inOffset+ 9] & 0xff) <<  8 | (in[inOffset+10] & 0xff) << 16 | (in[inOffset+11] & 0xff) << 24;
		x3 = (in[inOffset+12] & 0xff) | (in[inOffset+13] & 0xff) <<  8 | (in[inOffset+14] & 0xff) << 16 | (in[inOffset+15] & 0xff) << 24;

		x0 ^= k128; x1 ^= k129; x2 ^= k130; x3 ^= k131; sboxI7noLT();
		x3 ^= k124; x0 ^= k125; x1 ^= k126; x4 ^= k127; sboxI6();
		x0 ^= k120; x1 ^= k121; x2 ^= k122; x4 ^= k123; sboxI5();
		x1 ^= k116; x3 ^= k117; x4 ^= k118; x2 ^= k119; sboxI4();
		x1 ^= k112; x2 ^= k113; x4 ^= k114; x0 ^= k115; sboxI3();
		x0 ^= k108; x1 ^= k109; x4 ^= k110; x2 ^= k111; sboxI2();
		x1 ^= k104; x3 ^= k105; x4 ^= k106; x2 ^= k107; sboxI1();
		x0 ^= k100; x1 ^= k101; x2 ^= k102; x4 ^= k103; sboxI0();
		x0 ^= k96; x3 ^= k97; x1 ^= k98; x4 ^= k99; sboxI7();
		x1 = x3; x3 = x4; x4 = x2;

		x3 ^= k92; x0 ^= k93; x1 ^= k94; x4 ^= k95; sboxI6();
		x0 ^= k88; x1 ^= k89; x2 ^= k90; x4 ^= k91; sboxI5();
		x1 ^= k84; x3 ^= k85; x4 ^= k86; x2 ^= k87; sboxI4();
		x1 ^= k80; x2 ^= k81; x4 ^= k82; x0 ^= k83; sboxI3();
		x0 ^= k76; x1 ^= k77; x4 ^= k78; x2 ^= k79; sboxI2();
		x1 ^= k72; x3 ^= k73; x4 ^= k74; x2 ^= k75; sboxI1();
		x0 ^= k68; x1 ^= k69; x2 ^= k70; x4 ^= k71; sboxI0();
		x0 ^= k64; x3 ^= k65; x1 ^= k66; x4 ^= k67; sboxI7();
		x1 = x3; x3 = x4; x4 = x2;

		x3 ^= k60; x0 ^= k61; x1 ^= k62; x4 ^= k63; sboxI6();
		x0 ^= k56; x1 ^= k57; x2 ^= k58; x4 ^= k59; sboxI5();
		x1 ^= k52; x3 ^= k53; x4 ^= k54; x2 ^= k55; sboxI4();
		x1 ^= k48; x2 ^= k49; x4 ^= k50; x0 ^= k51; sboxI3();
		x0 ^= k44; x1 ^= k45; x4 ^= k46; x2 ^= k47; sboxI2();
		x1 ^= k40; x3 ^= k41; x4 ^= k42; x2 ^= k43; sboxI1();
		x0 ^= k36; x1 ^= k37; x2 ^= k38; x4 ^= k39; sboxI0();
		x0 ^= k32; x3 ^= k33; x1 ^= k34; x4 ^= k35; sboxI7();
		x1 = x3; x3 = x4; x4 = x2;

		x3 ^= k28; x0 ^= k29; x1 ^= k30; x4 ^= k31; sboxI6();
		x0 ^= k24; x1 ^= k25; x2 ^= k26; x4 ^= k27; sboxI5();
		x1 ^= k20; x3 ^= k21; x4 ^= k22; x2 ^= k23; sboxI4();
		x1 ^= k16; x2 ^= k17; x4 ^= k18; x0 ^= k19; sboxI3();
		x0 ^= k12; x1 ^= k13; x4 ^= k14; x2 ^= k15; sboxI2();
		x1 ^= k8; x3 ^= k9; x4 ^= k10; x2 ^= k11; sboxI1();
		x0 ^= k4; x1 ^= k5; x2 ^= k6; x4 ^= k7; sboxI0();
		x2 = x1; x1 = x3; x3 = x4;

		x0 ^= k0; x1 ^= k1; x2 ^= k2; x3 ^= k3;

		out[outOffset	] = (byte) x0;
		out[outOffset+ 1] = (byte)(x0 >>>  8);
		out[outOffset+ 2] = (byte)(x0 >>> 16);
		out[outOffset+ 3] = (byte)(x0 >>> 24);
		out[outOffset+ 4] = (byte) x1;
		out[outOffset+ 5] = (byte)(x1 >>>  8);
		out[outOffset+ 6] = (byte)(x1 >>> 16);
		out[outOffset+ 7] = (byte)(x1 >>> 24);
		out[outOffset+ 8] = (byte) x2;
		out[outOffset+ 9] = (byte)(x2 >>>  8);
		out[outOffset+10] = (byte)(x2 >>> 16);
		out[outOffset+11] = (byte)(x2 >>> 24);
		out[outOffset+12] = (byte) x3;
		out[outOffset+13] = (byte)(x3 >>>  8);
		out[outOffset+14] = (byte)(x3 >>> 16);
		out[outOffset+15] = (byte)(x3 >>> 24);
	}


	/**
	 * Encrypts a single block of ciphertext in ECB-mode.
	 *
	 * @param in
	 *	 A buffer containing the plaintext to be encrypted.
	 * @param inOffset
	 *	 Index in the in buffer where plaintext should be read.
	 * @param out
	 *	 A buffer where ciphertext is written.
	 * @param outOffset
	 *	 Index in the out buffer where ciphertext should be written.
	 */
	@Override
	public void engineEncryptBlock(int [] in, int inOffset, int [] out, int outOffset)
	{
		x0 = reverseBytes(in[inOffset++]);
		x1 = reverseBytes(in[inOffset++]);
		x2 = reverseBytes(in[inOffset++]);
		x3 = reverseBytes(in[inOffset  ]);

		x0 ^= k0; x1 ^= k1; x2 ^= k2; x3 ^= k3; sbox0();
		x1 ^= k4; x4 ^= k5; x2 ^= k6; x0 ^= k7; sbox1();
		x0 ^= k8; x4 ^= k9; x2 ^= k10; x1 ^= k11; sbox2();
		x2 ^= k12; x1 ^= k13; x4 ^= k14; x3 ^= k15; sbox3();
		x1 ^= k16; x4 ^= k17; x3 ^= k18; x0 ^= k19; sbox4();
		x4 ^= k20; x2 ^= k21; x1 ^= k22; x0 ^= k23; sbox5();
		x2 ^= k24; x0 ^= k25; x4 ^= k26; x1 ^= k27; sbox6();
		x2 ^= k28; x0 ^= k29; x3 ^= k30; x4 ^= k31; sbox7();
		x0 = x3; x3 = x2; x2 = x4;

		x0 ^= k32; x1 ^= k33; x2 ^= k34; x3 ^= k35; sbox0();
		x1 ^= k36; x4 ^= k37; x2 ^= k38; x0 ^= k39; sbox1();
		x0 ^= k40; x4 ^= k41; x2 ^= k42; x1 ^= k43; sbox2();
		x2 ^= k44; x1 ^= k45; x4 ^= k46; x3 ^= k47; sbox3();
		x1 ^= k48; x4 ^= k49; x3 ^= k50; x0 ^= k51; sbox4();
		x4 ^= k52; x2 ^= k53; x1 ^= k54; x0 ^= k55; sbox5();
		x2 ^= k56; x0 ^= k57; x4 ^= k58; x1 ^= k59; sbox6();
		x2 ^= k60; x0 ^= k61; x3 ^= k62; x4 ^= k63; sbox7();
		x0 = x3; x3 = x2; x2 = x4;

		x0 ^= k64; x1 ^= k65; x2 ^= k66; x3 ^= k67; sbox0();
		x1 ^= k68; x4 ^= k69; x2 ^= k70; x0 ^= k71; sbox1();
		x0 ^= k72; x4 ^= k73; x2 ^= k74; x1 ^= k75; sbox2();
		x2 ^= k76; x1 ^= k77; x4 ^= k78; x3 ^= k79; sbox3();
		x1 ^= k80; x4 ^= k81; x3 ^= k82; x0 ^= k83; sbox4();
		x4 ^= k84; x2 ^= k85; x1 ^= k86; x0 ^= k87; sbox5();
		x2 ^= k88; x0 ^= k89; x4 ^= k90; x1 ^= k91; sbox6();
		x2 ^= k92; x0 ^= k93; x3 ^= k94; x4 ^= k95; sbox7();
		x0 = x3; x3 = x2; x2 = x4;

		x0 ^= k96; x1 ^= k97; x2 ^= k98; x3 ^= k99; sbox0();
		x1 ^= k100; x4 ^= k101; x2 ^= k102; x0 ^= k103; sbox1();
		x0 ^= k104; x4 ^= k105; x2 ^= k106; x1 ^= k107; sbox2();
		x2 ^= k108; x1 ^= k109; x4 ^= k110; x3 ^= k111; sbox3();
		x1 ^= k112; x4 ^= k113; x3 ^= k114; x0 ^= k115; sbox4();
		x4 ^= k116; x2 ^= k117; x1 ^= k118; x0 ^= k119; sbox5();
		x2 ^= k120; x0 ^= k121; x4 ^= k122; x1 ^= k123; sbox6();
		x2 ^= k124; x0 ^= k125; x3 ^= k126; x4 ^= k127; sbox7noLT();
		x0 = x3; x3 = x2; x2 = x4;
		x0 ^= k128; x1 ^= k129; x2 ^= k130; x3 ^= k131;

		out[outOffset++] = reverseBytes(x0);
		out[outOffset++] = reverseBytes(x1);
		out[outOffset++] = reverseBytes(x2);
		out[outOffset  ] = reverseBytes(x3);
	}


	/**
	 * Decrypts a single block of ciphertext in ECB-mode.
	 *
	 * @param in
	 *   A buffer containing the ciphertext to be decrypted.
	 * @param inOffset
	 *   Index in the in buffer where ciphertext should be read.
	 * @param out
	 *   A buffer where plaintext is written.
	 * @param outOffset
	 *   Index in the out buffer where plaintext should be written.
	 */
	@Override
	public void engineDecryptBlock(int [] in, int inOffset, int [] out, int outOffset)
	{
		x0 = reverseBytes(in[inOffset++]);
		x1 = reverseBytes(in[inOffset++]);
		x2 = reverseBytes(in[inOffset++]);
		x3 = reverseBytes(in[inOffset  ]);

		x0 ^= k128; x1 ^= k129; x2 ^= k130; x3 ^= k131; sboxI7noLT();
		x3 ^= k124; x0 ^= k125; x1 ^= k126; x4 ^= k127; sboxI6();
		x0 ^= k120; x1 ^= k121; x2 ^= k122; x4 ^= k123; sboxI5();
		x1 ^= k116; x3 ^= k117; x4 ^= k118; x2 ^= k119; sboxI4();
		x1 ^= k112; x2 ^= k113; x4 ^= k114; x0 ^= k115; sboxI3();
		x0 ^= k108; x1 ^= k109; x4 ^= k110; x2 ^= k111; sboxI2();
		x1 ^= k104; x3 ^= k105; x4 ^= k106; x2 ^= k107; sboxI1();
		x0 ^= k100; x1 ^= k101; x2 ^= k102; x4 ^= k103; sboxI0();
		x0 ^= k96; x3 ^= k97; x1 ^= k98; x4 ^= k99; sboxI7();
		x1 = x3; x3 = x4; x4 = x2;

		x3 ^= k92; x0 ^= k93; x1 ^= k94; x4 ^= k95; sboxI6();
		x0 ^= k88; x1 ^= k89; x2 ^= k90; x4 ^= k91; sboxI5();
		x1 ^= k84; x3 ^= k85; x4 ^= k86; x2 ^= k87; sboxI4();
		x1 ^= k80; x2 ^= k81; x4 ^= k82; x0 ^= k83; sboxI3();
		x0 ^= k76; x1 ^= k77; x4 ^= k78; x2 ^= k79; sboxI2();
		x1 ^= k72; x3 ^= k73; x4 ^= k74; x2 ^= k75; sboxI1();
		x0 ^= k68; x1 ^= k69; x2 ^= k70; x4 ^= k71; sboxI0();
		x0 ^= k64; x3 ^= k65; x1 ^= k66; x4 ^= k67; sboxI7();
		x1 = x3; x3 = x4; x4 = x2;

		x3 ^= k60; x0 ^= k61; x1 ^= k62; x4 ^= k63; sboxI6();
		x0 ^= k56; x1 ^= k57; x2 ^= k58; x4 ^= k59; sboxI5();
		x1 ^= k52; x3 ^= k53; x4 ^= k54; x2 ^= k55; sboxI4();
		x1 ^= k48; x2 ^= k49; x4 ^= k50; x0 ^= k51; sboxI3();
		x0 ^= k44; x1 ^= k45; x4 ^= k46; x2 ^= k47; sboxI2();
		x1 ^= k40; x3 ^= k41; x4 ^= k42; x2 ^= k43; sboxI1();
		x0 ^= k36; x1 ^= k37; x2 ^= k38; x4 ^= k39; sboxI0();
		x0 ^= k32; x3 ^= k33; x1 ^= k34; x4 ^= k35; sboxI7();
		x1 = x3; x3 = x4; x4 = x2;

		x3 ^= k28; x0 ^= k29; x1 ^= k30; x4 ^= k31; sboxI6();
		x0 ^= k24; x1 ^= k25; x2 ^= k26; x4 ^= k27; sboxI5();
		x1 ^= k20; x3 ^= k21; x4 ^= k22; x2 ^= k23; sboxI4();
		x1 ^= k16; x2 ^= k17; x4 ^= k18; x0 ^= k19; sboxI3();
		x0 ^= k12; x1 ^= k13; x4 ^= k14; x2 ^= k15; sboxI2();
		x1 ^= k8; x3 ^= k9; x4 ^= k10; x2 ^= k11; sboxI1();
		x0 ^= k4; x1 ^= k5; x2 ^= k6; x4 ^= k7; sboxI0();
		x2 = x1; x1 = x3; x3 = x4;

		x0 ^= k0; x1 ^= k1; x2 ^= k2; x3 ^= k3;

		out[outOffset++] = reverseBytes(x0);
		out[outOffset++] = reverseBytes(x1);
		out[outOffset++] = reverseBytes(x2);
		out[outOffset  ] = reverseBytes(x3);
	}


	private void sbox0()
	{
		x3 ^= x0;
		x4 = x1;
		x1 &= x3;
		x4 ^= x2;
		x1 ^= x0;
		x0 |= x3;
		x0 ^= x4;
		x4 ^= x3;
		x3 ^= x2;
		x2 |= x1;
		x2 ^= x4;
		x4 ^= -1;
		x4 |= x1;
		x1 ^= x3;
		x1 ^= x4;
		x3 |= x0;
		x1 ^= x3;
		x4 ^= x3;

		x1 = (x1 << 13) | (x1 >>> 19);
		x4 ^= x1;
		x3 = x1 << 3;
		x2 = (x2 <<  3) | (x2 >>> 29);
		x4 ^= x2;
		x0 ^= x2;
		x4 = (x4 <<  1) | (x4 >>> 31);
		x0 ^= x3;
		x0 = (x0 <<  7) | (x0 >>> 25);
		x3 = x4;
		x1 ^= x4;
		x3 <<= 7;
		x1 ^= x0;
		x2 ^= x0;
		x2 ^= x3;
		x1 = (x1 <<  5) | (x1 >>> 27);
		x2 = (x2 << 22) | (x2 >>> 10);
	}

	private void sbox1()
	{
		x4 = ~x4;
		x3 = x1;
		x1 ^= x4;
		x3 |= x4;
		x3 ^= x0;
		x0 &= x1;
		x2 ^= x3;
		x0 ^= x4;
		x0 |= x2;
		x1 ^= x3;
		x0 ^= x1;
		x4 &= x2;
		x1 |= x4;
		x4 ^= x3;
		x1 ^= x2;
		x3 |= x0;
		x1 ^= x3;
		x3 = ~x3;
		x4 ^= x0;
		x3 &= x2;
		x4 = ~x4;
		x3 ^= x1;
		x4 ^= x3;

		x0 = (x0 << 13) | (x0 >>> 19);
		x4 ^= x0;
		x3 = x0 << 3;
		x2 = (x2 <<  3) | (x2 >>> 29);
		x4 ^= x2;
		x1 ^= x2;
		x4 = (x4 <<  1) | (x4 >>> 31);
		x1 ^= x3;
		x1 = (x1 <<  7) | (x1 >>> 25);
		x3 = x4;
		x0 ^= x4;
		x3 <<= 7;
		x0 ^= x1;
		x2 ^= x1;
		x2 ^= x3;
		x0 = (x0 <<  5) | (x0 >>> 27);
		x2 = (x2 << 22) | (x2 >>> 10);
	}

	private void sbox2()
	{
		x3 = x0;
		x0 = x0 & x2;
		x0 = x0 ^ x1;
		x2 = x2 ^ x4;
		x2 = x2 ^ x0;
		x1 = x1 | x3;
		x1 = x1 ^ x4;
		x3 = x3 ^ x2;
		x4 = x1;
		x1 = x1 | x3;
		x1 = x1 ^ x0;
		x0 = x0 & x4;
		x3 = x3 ^ x0;
		x4 = x4 ^ x1;
		x4 = x4 ^ x3;
		x3 = ~x3;

		x2 = (x2 << 13) | (x2 >>> 19);
		x1 ^= x2;
		x0 = x2 << 3;
		x4 = (x4 <<  3) | (x4 >>> 29);
		x1 ^= x4;
		x3 ^= x4;
		x1 = (x1 <<  1) | (x1 >>> 31);
		x3 ^= x0;
		x3 = (x3 <<  7) | (x3 >>> 25);
		x0 = x1;
		x2 ^= x1;
		x0 <<= 7;
		x2 ^= x3;
		x4 ^= x3;
		x4 ^= x0;
		x2 = (x2 <<  5) | (x2 >>> 27);
		x4 = (x4 << 22) | (x4 >>> 10);
	}

	private void sbox3()
	{
		x0 = x2;
		x2 = x2 | x3;
		x3 = x3 ^ x1;
		x1 = x1 & x0;
		x0 = x0 ^ x4;
		x4 = x4 ^ x3;
		x3 = x3 & x2;
		x0 = x0 | x1;
		x3 = x3 ^ x0;
		x2 = x2 ^ x1;
		x0 = x0 & x2;
		x1 = x1 ^ x3;
		x0 = x0 ^ x4;
		x1 = x1 | x2;
		x1 = x1 ^ x4;
		x2 = x2 ^ x3;
		x4 = x1;
		x1 = x1 | x3;
		x1 = x1 ^ x2;

		x1 = (x1 << 13) | (x1 >>> 19);
		x4 ^= x1;
		x2 = x1 << 3;
		x3 = (x3 <<  3) | (x3 >>> 29);
		x4 ^= x3;
		x0 ^= x3;
		x4 = (x4 <<  1) | (x4 >>> 31);
		x0 ^= x2;
		x0 = (x0 <<  7) | (x0 >>> 25);
		x2 = x4;
		x1 ^= x4;
		x2 <<= 7;
		x1 ^= x0;
		x3 ^= x0;
		x3 ^= x2;
		x1 = (x1 <<  5) | (x1 >>> 27);
		x3 = (x3 << 22) | (x3 >>> 10);
	}

	private void sbox4()
	{
		x4 = x4 ^ x0;
		x0 = ~x0;
		x3 = x3 ^ x0;
		x0 = x0 ^ x1;
		x2 = x4;
		x4 = x4 & x0;
		x4 = x4 ^ x3;
		x2 = x2 ^ x0;
		x1 = x1 ^ x2;
		x3 = x3 & x2;
		x3 = x3 ^ x1;
		x1 = x1 & x4;
		x0 = x0 ^ x1;
		x2 = x2 | x4;
		x2 = x2 ^ x1;
		x1 = x1 | x0;
		x1 = x1 ^ x3;
		x3 = x3 & x0;
		x1 = ~x1;
		x2 = x2 ^ x3;

		x4 = (x4 << 13) | (x4 >>> 19);
		x2 ^= x4;
		x3 = x4 << 3;
		x1 = (x1 <<  3) | (x1 >>> 29);
		x2 ^= x1;
		x0 ^= x1;
		x2 = (x2 <<  1) | (x2 >>> 31);
		x0 ^= x3;
		x0 = (x0 <<  7) | (x0 >>> 25);
		x3 = x2;
		x4 ^= x2;
		x3 <<= 7;
		x4 ^= x0;
		x1 ^= x0;
		x1 ^= x3;
		x4 = (x4 <<  5) | (x4 >>> 27);
		x1 = (x1 << 22) | (x1 >>> 10);
	}

	private void sbox5()
	{
		x4 = x4 ^ x2;
		x2 = x2 ^ x0;
		x0 = ~x0;
		x3 = x2;
		x2 = x2 & x4;
		x1 = x1 ^ x0;
		x2 = x2 ^ x1;
		x1 = x1 | x3;
		x3 = x3 ^ x0;
		x0 = x0 & x2;
		x0 = x0 ^ x4;
		x3 = x3 ^ x2;
		x3 = x3 ^ x1;
		x1 = x1 ^ x4;
		x4 = x4 & x0;
		x1 = ~x1;
		x4 = x4 ^ x3;
		x3 = x3 | x0;
		x1 = x1 ^ x3;

		x2 = (x2 << 13) | (x2 >>> 19);
		x0 ^= x2;
		x3 = x2 << 3;
		x4 = (x4 <<  3) | (x4 >>> 29);
		x0 ^= x4;
		x1 ^= x4;
		x0 = (x0 <<  1) | (x0 >>> 31);
		x1 ^= x3;
		x1 = (x1 <<  7) | (x1 >>> 25);
		x3 = x0;
		x2 ^= x0;
		x3 <<= 7;
		x2 ^= x1;
		x4 ^= x1;
		x4 ^= x3;
		x2 = (x2 <<  5) | (x2 >>> 27);
		x4 = (x4 << 22) | (x4 >>> 10);
	}

	private void sbox6()
	{
		x4 = ~x4;
		x3 = x1;
		x1 = x1 & x2;
		x2 = x2 ^ x3;
		x1 = x1 ^ x4;
		x4 = x4 | x3;
		x0 = x0 ^ x1;
		x4 = x4 ^ x2;
		x2 = x2 | x0;
		x4 = x4 ^ x0;
		x3 = x3 ^ x2;
		x2 = x2 | x1;
		x2 = x2 ^ x4;
		x3 = x3 ^ x1;
		x3 = x3 ^ x2;
		x1 = ~x1;
		x4 = x4 & x3;
		x4 = x4 ^ x1;
		x2 = (x2 << 13) | (x2 >>> 19);
		x0 ^= x2;
		x1 = x2 << 3;
		x3 = (x3 <<  3) | (x3 >>> 29);
		x0 ^= x3;
		x4 ^= x3;
		x0 = (x0 <<  1) | (x0 >>> 31);
		x4 ^= x1;
		x4 = (x4 <<  7) | (x4 >>> 25);
		x1 = x0;
		x2 ^= x0;
		x1 <<= 7;
		x2 ^= x4;
		x3 ^= x4;
		x3 ^= x1;
		x2 = (x2 <<  5) | (x2 >>> 27);
		x3 = (x3 << 22) | (x3 >>> 10);
	}

	private void sbox7()
	{
		x1 = x3;
		x3 = x3 & x0;
		x3 = x3 ^ x4;
		x4 = x4 & x0;
		x1 = x1 ^ x3;
		x3 = x3 ^ x0;
		x0 = x0 ^ x2;
		x2 = x2 | x1;
		x2 = x2 ^ x3;
		x4 = x4 ^ x0;
		x3 = x3 ^ x4;
		x4 = x4 & x2;
		x4 = x4 ^ x1;
		x1 = x1 ^ x3;
		x3 = x3 & x2;
		x1 = ~x1;
		x3 = x3 ^ x1;
		x1 = x1 & x2;
		x0 = x0 ^ x4;
		x1 = x1 ^ x0;
		x3 = (x3 << 13) | (x3 >>> 19);
		x1 ^= x3;
		x0 = x3 << 3;
		x4 = (x4 <<  3) | (x4 >>> 29);
		x1 ^= x4;
		x2 ^= x4;
		x1 = (x1 <<  1) | (x1 >>> 31);
		x2 ^= x0;
		x2 = (x2 <<  7) | (x2 >>> 25);
		x0 = x1;
		x3 ^= x1;
		x0 <<= 7;
		x3 ^= x2;
		x4 ^= x2;
		x4 ^= x0;
		x3 = (x3 <<  5) | (x3 >>> 27);
		x4 = (x4 << 22) | (x4 >>> 10);
	}

	/** The final S-box, with no transform. */
	private void sbox7noLT()
	{
		x1 = x3;
		x3 = x3 & x0;
		x3 = x3 ^ x4;
		x4 = x4 & x0;
		x1 = x1 ^ x3;
		x3 = x3 ^ x0;
		x0 = x0 ^ x2;
		x2 = x2 | x1;
		x2 = x2 ^ x3;
		x4 = x4 ^ x0;
		x3 = x3 ^ x4;
		x4 = x4 & x2;
		x4 = x4 ^ x1;
		x1 = x1 ^ x3;
		x3 = x3 & x2;
		x1 = ~x1;
		x3 = x3 ^ x1;
		x1 = x1 & x2;
		x0 = x0 ^ x4;
		x1 = x1 ^ x0;
	}

	private void sboxI7noLT()
	{
		x4 = x2;
		x2 ^= x0;
		x0 &= x3;
		x2 = ~x2;
		x4 |= x3;
		x3 ^= x1;
		x1 |= x0;
		x0 ^= x2;
		x2 &= x4;
		x1 ^= x2;
		x2 ^= x0;
		x0 |= x2;
		x3 &= x4;
		x0 ^= x3;
		x4 ^= x1;
		x3 ^= x4;
		x4 |= x0;
		x3 ^= x2;
		x4 ^= x2;
	}

	private void sboxI6()
	{
		x1 = (x1 >>> 22) | (x1 << 10);
		x3 = (x3 >>>  5) | (x3 << 27);
		x2 = x0;
		x1 ^= x4;
		x2 <<= 7;
		x3 ^= x4;
		x1 ^= x2;
		x3 ^= x0;
		x4 = (x4 >>>  7) | (x4 << 25);
		x0 = (x0 >>>  1) | (x0 << 31);
		x0 ^= x3;
		x2 = x3 << 3;
		x4 ^= x2;
		x3 = (x3 >>> 13) | (x3 << 19);
		x0 ^= x1;
		x4 ^= x1;
		x1 = (x1 >>>  3) | (x1 << 29);
		x3 ^= x1;
		x2 = x1;
		x1 &= x3;
		x2 ^= x4;
		x1 = ~x1;
		x4 ^= x0;
		x1 ^= x4;
		x2 |= x3;
		x3 ^= x1;
		x4 ^= x2;
		x2 ^= x0;
		x0 &= x4;
		x0 ^= x3;
		x3 ^= x4;
		x3 |= x1;
		x4 ^= x0;
		x2 ^= x3;
	}

	private void sboxI5()
	{
		x2 = (x2 >>> 22) | (x2 << 10);
		x0 = (x0 >>>  5) | (x0 << 27);
		x3 = x1;
		x2 ^= x4;
		x3 <<= 7;
		x0 ^= x4;
		x2 ^= x3;
		x0 ^= x1;
		x4 = (x4 >>>  7) | (x4 << 25);
		x1 = (x1 >>>  1) | (x1 << 31);
		x1 ^= x0;
		x3 = x0 << 3;
		x4 ^= x3;
		x0 = (x0 >>> 13) | (x0 << 19);
		x1 ^= x2;
		x4 ^= x2;
		x2 = (x2 >>>  3) | (x2 << 29);
		x1 = ~x1;
		x3 = x4;
		x2 ^= x1;
		x4 |= x0;
		x4 ^= x2;
		x2 |= x1;
		x2 &= x0;
		x3 ^= x4;
		x2 ^= x3;
		x3 |= x0;
		x3 ^= x1;
		x1 &= x2;
		x1 ^= x4;
		x3 ^= x2;
		x4 &= x3;
		x3 ^= x1;
		x4 ^= x0;
		x4 ^= x3;
		x3 = ~x3;
	}

	private void sboxI4()
	{
		x4 = (x4 >>> 22) | (x4 << 10);
		x1 = (x1 >>>  5) | (x1 << 27);
		x0 = x3;
		x4 ^= x2;
		x0 <<= 7;
		x1 ^= x2;
		x4 ^= x0;
		x1 ^= x3;
		x2 = (x2 >>>  7) | (x2 << 25);
		x3 = (x3 >>>  1) | (x3 << 31);
		x3 ^= x1;
		x0 = x1 << 3;
		x2 ^= x0;
		x1 = (x1 >>> 13) | (x1 << 19);
		x3 ^= x4;
		x2 ^= x4;
		x4 = (x4 >>>  3) | (x4 << 29);
		x0 = x4;
		x4 &= x2;
		x4 ^= x3;
		x3 |= x2;
		x3 &= x1;
		x0 ^= x4;
		x0 ^= x3;
		x3 &= x4;
		x1 = ~x1;
		x2 ^= x0;
		x3 ^= x2;
		x2 &= x1;
		x2 ^= x4;
		x1 ^= x3;
		x4 &= x1;
		x2 ^= x1;
		x4 ^= x0;
		x4 |= x2;
		x2 ^= x1;
		x4 ^= x3;
	}

	private void sboxI3()
	{
		x4 = (x4 >>> 22) | (x4 << 10);
		x1 = (x1 >>>  5) | (x1 << 27);
		x3 = x2;
		x4 ^= x0;
		x3 <<= 7;
		x1 ^= x0;
		x4 ^= x3;
		x1 ^= x2;
		x0 = (x0 >>>  7) | (x0 << 25);
		x2 = (x2 >>>  1) | (x2 << 31);
		x2 ^= x1;
		x3 = x1 << 3;
		x0 ^= x3;
		x1 = (x1 >>> 13) | (x1 << 19);
		x2 ^= x4;
		x0 ^= x4;
		x4 = (x4 >>>  3) | (x4 << 29);
		x3 = x4;
		x4 ^= x2;
		x2 &= x4;
		x2 ^= x1;
		x1 &= x3;
		x3 ^= x0;
		x0 |= x2;
		x0 ^= x4;
		x1 ^= x3;
		x4 ^= x1;
		x1 |= x0;
		x1 ^= x2;
		x3 ^= x4;
		x4 &= x0;
		x2 |= x0;
		x2 ^= x4;
		x3 ^= x1;
		x4 ^= x3;
	}

	private void sboxI2()
	{
		x4 = (x4 >>> 22) | (x4 << 10);
		x0 = (x0 >>>  5) | (x0 << 27);
		x3 = x1;
		x4 ^= x2;
		x3 <<= 7;
		x0 ^= x2;
		x4 ^= x3;
		x0 ^= x1;
		x2 = (x2 >>>  7) | (x2 << 25);
		x1 = (x1 >>>  1) | (x1 << 31);
		x1 ^= x0;
		x3 = x0 << 3;
		x2 ^= x3;
		x0 = (x0 >>> 13) | (x0 << 19);
		x1 ^= x4;
		x2 ^= x4;
		x4 = (x4 >>>  3) | (x4 << 29);
		x4 ^= x2;
		x2 ^= x0;
		x3 = x2;
		x2 &= x4;
		x2 ^= x1;
		x1 |= x4;
		x1 ^= x3;
		x3 &= x2;
		x4 ^= x2;
		x3 &= x0;
		x3 ^= x4;
		x4 &= x1;
		x4 |= x0;
		x2 = ~x2;
		x4 ^= x2;
		x0 ^= x2;
		x0 &= x1;
		x2 ^= x3;
		x2 ^= x0;
	}

	private void sboxI1()
	{
		x4 = (x4 >>> 22) | (x4 << 10);
		x1 = (x1 >>>  5) | (x1 << 27);
		x0 = x3;
		x4 ^= x2;
		x0 <<= 7;
		x1 ^= x2;
		x4 ^= x0;
		x1 ^= x3;
		x2 = (x2 >>>  7) | (x2 << 25);
		x3 = (x3 >>>  1) | (x3 << 31);
		x3 ^= x1;
		x0 = x1 << 3;
		x2 ^= x0;
		x1 = (x1 >>> 13) | (x1 << 19);
		x3 ^= x4;
		x2 ^= x4;
		x4 = (x4 >>>  3) | (x4 << 29);
		x0 = x3;
		x3 ^= x2;
		x2 &= x3;
		x0 ^= x4;
		x2 ^= x1;
		x1 |= x3;
		x4 ^= x2;
		x1 ^= x0;
		x1 |= x4;
		x3 ^= x2;
		x1 ^= x3;
		x3 |= x2;
		x3 ^= x1;
		x0 = ~x0;
		x0 ^= x3;
		x3 |= x1;
		x3 ^= x1;
		x3 |= x0;
		x2 ^= x3;
	}

	private void sboxI0()
	{
		x2 = (x2 >>> 22) | (x2 << 10);
		x0 = (x0 >>>  5) | (x0 << 27);
		x3 = x1;
		x2 ^= x4;
		x3 <<= 7;
		x0 ^= x4;
		x2 ^= x3;
		x0 ^= x1;
		x4 = (x4 >>>  7) | (x4 << 25);
		x1 = (x1 >>>  1) | (x1 << 31);
		x1 ^= x0;
		x3 = x0 << 3;
		x4 ^= x3;
		x0 = (x0 >>> 13) | (x0 << 19);
		x1 ^= x2;
		x4 ^= x2;
		x2 = (x2 >>>  3) | (x2 << 29);
		x2 = ~x2;
		x3 = x1;
		x1 |= x0;
		x3 = ~x3;
		x1 ^= x2;
		x2 |= x3;
		x1 ^= x4;
		x0 ^= x3;
		x2 ^= x0;
		x0 &= x4;
		x3 ^= x0;
		x0 |= x1;
		x0 ^= x2;
		x4 ^= x3;
		x2 ^= x1;
		x4 ^= x0;
		x4 ^= x1;
		x2 &= x4;
		x3 ^= x2;
	}

	private void sboxI7()
	{
		x1 = (x1 >>> 22) | (x1 << 10);
		x0 = (x0 >>>  5) | (x0 << 27);
		x2 = x3;
		x1 ^= x4;
		x2 <<= 7;
		x0 ^= x4;
		x1 ^= x2;
		x0 ^= x3;
		x4 = (x4 >>>  7) | (x4 << 25);
		x3 = (x3 >>>  1) | (x3 << 31);
		x3 ^= x0;
		x2 = x0 << 3;
		x4 ^= x2;
		x0 = (x0 >>> 13) | (x0 << 19);
		x3 ^= x1;
		x4 ^= x1;
		x1 = (x1 >>>  3) | (x1 << 29);
		x2 = x1;
		x1 ^= x0;
		x0 &= x4;
		x1 = ~x1;
		x2 |= x4;
		x4 ^= x3;
		x3 |= x0;
		x0 ^= x1;
		x1 &= x2;
		x3 ^= x1;
		x1 ^= x0;
		x0 |= x1;
		x4 &= x2;
		x0 ^= x4;
		x2 ^= x3;
		x4 ^= x2;
		x2 |= x0;
		x4 ^= x1;
		x2 ^= x1;
	}

	// These S-Box functions are used in the key setup.

	/** S-Box 0. */
	private void sbox0(int r0, int r1, int r2, int r3)
	{
		int r4 = r1 ^ r2;
		r3 ^= r0;
		r1 = r1 & r3 ^ r0;
		r0 = (r0 | r3) ^ r4;
		r4 ^= r3;
		r3 ^= r2;
		r2 = (r2 | r1) ^ r4;
		r4 = ~r4 | r1;
		r1 ^= r3 ^ r4;
		r3 |= r0;
		x0 = r1 ^ r3;
		x1 = r4 ^ r3;
		x2 = r2;
		x3 = r0;
	}

	/** S-Box 1. */
	private void sbox1(int r0, int r1, int r2, int r3)
	{
		r0 = ~r0;
		int r4 = r0;
		r2 = ~r2;
		r0 &= r1;
		r2 ^= r0;
		r0 |= r3;
		r3 ^= r2;
		r1 ^= r0;
		r0 ^= r4;
		r4 |= r1;
		r1 ^= r3;
		r2 = (r2 | r0) & r4;
		r0 ^= r1;
		x0 = r2;
		x1 = r0 & r2 ^ r4;
		x2 = r3;
		x3 = r1 & r2 ^ r0;
	}

	/** S-Box 2. */
	private void sbox2(int r0, int r1, int r2, int r3)
	{
		int r4 = r0;
		r0 = r0 & r2 ^ r3;
		r2 = r2 ^ r1 ^ r0;
		r3 = (r3 | r4) ^ r1;
		r4 ^= r2;
		r1 = r3;
		r3 = (r3 | r4) ^ r0;
		r0 &= r1;
		r4 ^= r0;
		x0 = r2;
		x1 = r3;
		x2 = r1 ^ r3 ^ r4;
		x3 = ~r4;
	}

	/** S-Box 3. */
	private void sbox3(int r0, int r1, int r2, int r3)
	{
		int r4 = r0;
		r0 |= r3;
		r3 ^= r1;
		r1 &= r4;
		r4 = r4 ^ r2 | r1;
		r2 ^= r3;
		r3 = r3 & r0 ^ r4;
		r0 ^= r1;
		r4 = r4 & r0 ^ r2;
		r1 = (r1 ^ r3 | r0) ^ r2;
		r0 ^= r3;
		x0 = (r1 | r3) ^ r0;
		x1 = r1;
		x2 = r3;
		x3 = r4;
	}

	/** S-Box 4. */
	private void sbox4(int r0, int r1, int r2, int r3)
	{
		r1 ^= r3;
		int r4 = r1;
		r3 = ~r3;
		r2 ^= r3;
		r3 ^= r0;
		r1 = r1 & r3 ^ r2;
		r4 ^= r3;
		r0 ^= r4;
		r2 = r2 & r4 ^ r0;
		r0 &= r1;
		r3 ^= r0;
		r4 = (r4 | r1) ^ r0;
		x0 = r1;
		x1 = r4 ^ (r2 & r3);
		x2 = ~((r0 | r3) ^ r2);
		x3 = r3;
	}

	/** S-Box 5. */
	private void sbox5(int r0, int r1, int r2, int r3)
	{
		r0 ^= r1;
		r1 ^= r3;
		int r4 = r1;
		r3 = ~r3;
		r1 &= r0;
		r2 ^= r3;
		r1 ^= r2;
		r2 |= r4;
		r4 ^= r3;
		r3 = r3 & r1 ^ r0;
		r4 = r4 ^ r1 ^ r2;
		x0 = r1;
		x1 = r3;
		x2 = r0 & r3 ^ r4;
		x3 = ~(r2 ^ r0) ^ (r4 | r3);
	}

	/** S-Box 6. */
	private void sbox6(int r0, int r1, int r2, int r3)
	{
		int r4 = r3;
		r2 = ~r2;
		r3 = r3 & r0 ^ r2;
		r0 ^= r4;
		r2 = (r2 | r4) ^ r0;
		r1 ^= r3;
		r0 |= r1;
		r2 ^= r1;
		r4 ^= r0;
		r0 = (r0 | r3) ^ r2;
		r4 = r4 ^ r3 ^ r0;
		x0 = r0;
		x1 = r1;
		x2 = r4;
		x3 = r2 & r4 ^ ~r3;
	}

	/** S-Box 7. */
	private void sbox7(int r0, int r1, int r2, int r3)
	{
		int r4 = r1;
		r1 = (r1 | r2) ^ r3;
		r4 ^= r2;
		r2 ^= r1;
		r3 = (r3 | r4) & r0;
		r4 ^= r2;
		r3 ^= r1;
		r1 = (r1 | r4) ^ r0;
		r0 = (r0 | r4) ^ r2;
		r1 ^= r4;
		r2 ^= r1;
		x0 = r4 ^ (~r2 | r0);
		x1 = r3;
		x2 = r1 & r0 ^ r4;
		x3 = r0;
	}


	/**
	 * Returns the block size.
	 */
	@Override
	public int engineGetBlockSize()
	{
		return DEFAULT_BLOCK_SIZE;
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
		x0 = x1 = x2 = x3 = x4 = 0;
		k0 = k1 = k2 = k3 = k4 = k5 = k6 = k7 = k8 = k9 = k10 = k11 = k12 = k13 =
		k14 = k15 = k16 = k17 = k18 = k19 = k20 = k21 = k22 = k23 = k24 = k25 = k26 =
		k27 = k28 = k29 = k30 = k31 = k32 = k33 = k34 = k35 = k36 = k37 = k38 = k39 =
		k40 = k41 = k42 = k43 = k44 = k45 = k46 = k47 = k48 = k49 = k50 = k51 = k52 =
		k53 = k54 = k55 = k56 = k57 = k58 = k59 = k60 = k61 = k62 = k63 = k64 = k65 =
		k66 = k67 = k68 = k69 = k70 = k71 = k72 = k73 = k74 = k75 = k76 = k77 = k78 =
		k79 = k80 = k81 = k82 = k83 = k84 = k85 = k86 = k87 = k88 = k89 = k90 = k91 =
		k92 = k93 = k94 = k95 = k96 = k97 = k98 = k99 = k100 = k101 = k102 = k103 =
		k104 = k105 = k106 = k107 = k108 = k109 = k110 = k111 = k112 = k113 = k114 =
		k115 = k116 = k117 = k118 = k119 = k120 = k121 = k122 = k123 = k124 = k125 =
		k126 = k127 = k128 = k129 = k130 = k131 = 0;
	}


	@Override
	public String toString()
	{
		return "Serpent";
	}


	/**
	 * This method returns a new instance of the Twofish cipher.
	 */
	@Override
	public Cipher newInstance()
	{
		return new Serpent();
	}


    private static int reverseBytes(int i)
    {
        return ((i >>> 24)           ) +
               ((i >>   8) &   0xFF00) +
               ((i <<   8) & 0xFF0000) +
               ((i << 24));
    }
}
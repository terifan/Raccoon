package org.terifan.security.cryptography;

import org.terifan.security.messagedigest.HMAC;
import org.terifan.security.messagedigest.SHA512;
import static org.testng.Assert.*;
import org.testng.annotations.Test;
import resources.__TestUtils;


public class SCryptNGTest
{
	@Test
	public void testSomeMethod()
	{
		byte[] output1 = SCrypt.generate(new HMAC(new SHA512(), "".getBytes()), "".getBytes(), 16, 1, 1, 1, 64);
		assertEquals(output1, __TestUtils.hexToBytes("ae54e774e4516b0fe1e7280317e48cfa2f66557fdc3b40ab4784c96336079de586439589b6c06c726400c12ad76921928ebaa4599f00143a7c12589109a032fe"));

		byte[] output2 = SCrypt.generate(new HMAC(new SHA512(), "password".getBytes()), "NaCl".getBytes(), 1024, 8, 16, 1, 64);
		assertEquals(output2, __TestUtils.hexToBytes("c5b3d6ea0a4b1ecc4000e5985cdc060678349216cfe49f03962d4135009bff7460196ee6a646f737cbfad09f80722e85133e1a919053a1338551dc621c0e4d30"));

		byte[] output3 = SCrypt.generate(new HMAC(new SHA512(), "pleaseletmein".getBytes()), "SodiumChloride".getBytes(), 16384, 8, 1, 1, 64);
		assertEquals(output3, __TestUtils.hexToBytes("84a3eac52ac434a26536b7f7f128aa74bc827528c6cb646c707989bfa58dd61f4d5b69e7089d7fe17c1ac5ded74b04cb205e869c8c026dce4f32a4bcefda25ba"));

		byte[] output4 = SCrypt.generate(new HMAC(new SHA512(), "pleaseletmein".getBytes()), "SodiumChloride".getBytes(), 16384, 16, 1, 8192, 160);
	}
}

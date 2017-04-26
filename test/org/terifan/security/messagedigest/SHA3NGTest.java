package org.terifan.security.messagedigest;

import java.security.MessageDigest;
import static org.testng.Assert.*;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class SHA3NGTest
{
	@Test(dataProvider = "data")
	public void testSHA3(int aLength, byte[] aInput, byte[] aExpected)
	{
		MessageDigest digest = new SHA3(aLength);

		digest.update("dummy".getBytes()); // result ignored
		digest.digest();

		digest.update(aInput);
		byte[] out = digest.digest();

		assertEquals(out, aExpected);
	}


	@DataProvider(name = "data")
	private Object[][] data()
	{
		return new Object[][]
		{
			{
				224, "".getBytes(), unhex("6b4e03423667dbb73b6e15454f0eb1abd4597f9a1b078e3f5b5a6bc7")
			},
			{
				256, "".getBytes(), unhex("a7ffc6f8bf1ed76651c14756a061d662f580ff4de43b49fa82d80a4b80f8434a")
			},
			{
				384, "".getBytes(), unhex("0c63a75b845e4f7d01107d852e4c2485c51a50aaaa94fc61995e71bbee983a2ac3713831264adb47fb6bd1e058d5f004")
			},
			{
				512, "".getBytes(), unhex("a69f73cca23a9ac5c8b567dc185a756e97c982164fe25859e0d1dcc1475c80a615b2123af1f5f94c11e3e9402c3ac558f500199d95b6d3e301758586281dcd26")
			},
			{
				512, "abc".getBytes(), unhex("b751850b1a57168a5693cd924b6b096e08f621827444f70d884f5d0240d2712e10e116e9192af3c91a7ec57647e3934057340b4cf408d5a56592f8274eec53f0")
			},
			{
				512, "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq".getBytes(), unhex("04a371e84ecfb5b8b77cb48610fca8182dd457ce6f326a0fd3d7ec2f1e91636dee691fbe0c985302ba1b0d8dc78c086346b533b49c030d99a27daf1139d6e75e")
			},
			{
				512, "abcdefghbcdefghicdefghijdefghijkefghijklfghijklmghijklmnhijklmnoijklmnopjklmnopqklmnopqrlmnopqrsmnopqrstnopqrstu".getBytes(), unhex("afebb2ef542e6579c50cad06d2e578f9f8dd6881d7dc824d26360feebf18a4fa73e3261122948efcfd492e74e82e2189ed0fb440d187f382270cb455f21dd185")
			}
		};
	}


	private static byte[] unhex(String in)
	{
		byte[] out = new byte[in.length() / 2];
		for (int i = 0; i < out.length; i++)
		{
			out[i] = (byte)Integer.parseInt(in.substring(2 * i, 2 * i + 2), 16);
		}
		return out;
	}
}

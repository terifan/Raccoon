package longtests;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.terifan.raccoon.ArrayMap;
import org.terifan.raccoon.ArrayMapEntry;
import static org.terifan.raccoon.ArrayMapNGTest.fillArrayMap;
import static org.testng.Assert.assertTrue;
import static resources.__TestUtils.rnd;


public class TestNGTest
{
//	@org.testng.annotations.Test
	public void testFillBuffer() throws UnsupportedEncodingException
	{
		for (;;)
		{
			long seed = rnd.nextLong();
//			long seed = -4300988232130434538L;
			rnd.setSeed(seed);

			ArrayMap map = new ArrayMap(50 + rnd.nextInt(1000));

			HashMap<String, byte[]> expected = new HashMap<>();

			fillArrayMap(map, expected);

			for (Map.Entry<String, byte[]> expectedEntry : expected.entrySet())
			{
				ArrayMapEntry entry = new ArrayMapEntry(expectedEntry.getKey().getBytes("utf-8"));

				if (!map.get(entry) || !Arrays.equals(entry.getValue(), expectedEntry.getValue()))
				{
					System.out.println(seed);

					assertTrue(false);
				}
			}
		}
	}
}

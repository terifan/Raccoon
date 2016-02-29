package tests;

import org.terifan.raccoon.serialization.old.FieldCategory;
import org.terifan.raccoon.serialization.old.Marshaller;
import org.terifan.raccoon.serialization.old.TypeDeclarations;
import org.terifan.raccoon.util.Log;


public class Marshalling
{
	public static void main(String... args)
	{
		try
		{
			Log.LEVEL = 10;

			_BigObject1K in = new _BigObject1K();
			in.random();

			Marshaller marshaller = new Marshaller(_BigObject1K.class);
			TypeDeclarations types = marshaller.getTypeDeclarations();
			byte[] buffer = marshaller.marshal(in, FieldCategory.VALUES);

			Log.out.println("---------");
			Log.out.println(types);
			Log.out.println("---------");
			Log.hexDump(buffer);
			Log.out.println("---------");

			_BigObject1K out = new _BigObject1K();

			new Marshaller(types).unmarshal(buffer, out, FieldCategory.VALUES);

			Log.out.println(in.mStringMapArray.keySet().iterator().next()[0]);
			Log.out.println(out.mStringMapArray.keySet().iterator().next()[0]);
			Log.out.println(in.mStringMapArray.values().iterator().next()[0]);
			Log.out.println(out.mStringMapArray.values().iterator().next()[0]);
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}
}

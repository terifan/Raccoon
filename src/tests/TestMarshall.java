package tests;


public class TestMarshall
{
	public static void main(String... args)
	{
		try
		{
//			Log.LEVEL = 10;
//
//			byte[] formatData;
//			byte[] entryData;
//
//			{
//				Object object = new _BooleansK1(new byte[]{1}, new boolean[]{true,false}, new boolean[][]{{true,false},{true,true}}, new boolean[][][]{{{true,false},{true,false}},{{true,false},{true,false}}});
////				Object object = new _BigObject1K().random();
////				Object object = new _Number1K2D(15, "red", 12, "apple");
//
//				EntityDescriptor td = new EntityDescriptor(object.getClass());
//				ByteArrayOutputStream baos = new ByteArrayOutputStream();
//				try (ObjectOutputStream oos = new ObjectOutputStream(baos))
//				{
//					td.writeExternal(oos);
//				}
//
//				formatData = baos.toByteArray();
//
//				Marshaller marshaller = new Marshaller(td);
//
//				ByteArrayBuffer buffer = new ByteArrayBuffer(16);
//				marshaller.marshal(buffer, object, FieldCategoryFilter.ALL);
//
//				entryData = buffer.trim().array();
//			}
//
//			Log.hexDump(entryData);
//
//			EntityDescriptor td = new EntityDescriptor();
//			td.readExternal(new ObjectInputStream(new ByteArrayInputStream(formatData)));
//			td.mapFields(Class.forName(td.getName()));
//
//			Marshaller marshaller = new Marshaller(td);
//
//			Object object = Class.forName(td.getName()).newInstance();
//			marshaller.unmarshal(entryData, object, FieldCategoryFilter.ALL);
//			Log.out.println(object);
//
//			ByteArrayOutputStream baos = new ByteArrayOutputStream();
//			try (ObjectOutputStream oos = new ObjectOutputStream(baos))
//			{
//				oos.writeObject(object);
//			}
//			Log.hexDump(baos.toByteArray());
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}
}

package org.terifan.raccoon.document;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import org.terifan.raccoon.ObjectId;
import java.util.UUID;
import org.terifan.raccoon.util.Log;
import static org.testng.Assert.*;
import org.testng.annotations.Test;


public class DocumentNGTest
{
	@Test
	public void testSomeMethod()
	{
		Document source = new Document().put("_id", 1).put("text", "hello").put("array", Array.of(1, 2, 3));

		byte[] data = source.toByteArray();

//		Log.hexDump(data);

		Document unmarshaled = new Document().fromByteArray(data);

		assertEquals(unmarshaled, source);
	}


	@Test
	public void testDateTimeTypes()
	{
		OffsetDateTime odt = OffsetDateTime.now();

		byte[] data = new Document()
			.put("offset", odt)
			.put("date", odt.toLocalDate())
			.put("time", odt.toLocalTime())
			.put("datetime", odt.toLocalDateTime())
			.toByteArray();

		Document doc = new Document().fromByteArray(data);

//		Log.hexDump(data);

		assertEquals(doc.getOffsetDateTime("offset"), odt);
		assertEquals(doc.getDate("date"), odt.toLocalDate());
		assertEquals(doc.getTime("time"), odt.toLocalTime());
		assertEquals(doc.getDateTime("datetime"), odt.toLocalDateTime());
	}


	@Test
	public void testObjectId()
	{
		ObjectId id = ObjectId.randomId();

		byte[] data = new Document()
			.put("_id", id)
			.toByteArray();

		Document doc = new Document().fromByteArray(data);

//		Log.hexDump(data);

		assertEquals(doc.get("_id"), id);
		assertEquals(doc.getObjectId("_id"), id);
	}


	@Test
	public void testAllTypes()
	{
		Byte _byte = Byte.MAX_VALUE;
		Short _short = Short.MAX_VALUE;
		Integer _int = Integer.MAX_VALUE;
		Float _float = 3.14f;
		Long _long = Long.MAX_VALUE;
		Double _double = Math.PI;
		Boolean _bool = true;
		Object _null = null;
		String _string = "hello";
		byte[] _bytes = "world".getBytes();
		UUID _uuid = UUID.randomUUID();
		OffsetDateTime _odt = OffsetDateTime.now();
		LocalDate _ld = LocalDate.now();
		LocalTime _lt = LocalTime.now();
		LocalDateTime _ldt = LocalDateTime.now();
		Array _arr = Array.of((byte)1,(byte)2,(byte)3); // JSON decoder decodes values to smallest possible representation
		Document _doc = new Document().put("docu","ment");
		BigDecimal _bd = new BigDecimal("31.31646131940661321981");

		Document _allTypesDoc = new Document()
			.put("byte", _byte)
			.put("short", _short)
			.put("int", _int)
			.put("long", _long)
			.put("float", _float)
			.put("double", _double)
			.put("bool", _bool)
			.put("null", _null)
			.put("string", _string)
			.put("bytes", _bytes)
			.put("uuid", _uuid)
			.put("odt", _odt)
			.put("ld", _ld)
			.put("lt", _lt)
			.put("ldt", _ldt)
			.put("arr", _arr)
			.put("doc", _doc)
			.put("bd", _bd);

		Array _allTypesArr = Array.of(_allTypesDoc.values());

		Document srcDoc = new Document()
			.put("doc", _allTypesDoc)
			.put("arr", _allTypesArr);

		byte[] data = srcDoc.toByteArray();
		String json = srcDoc.toJson();
		String text = srcDoc.toString();

		Document unmarshalledBin = new Document().fromByteArray(data);
		Document dstDoc = unmarshalledBin.get("doc");
		Array dstArr = unmarshalledBin.get("arr");

		Document unmarshalledJson = new Document().fromJson(json);
		Document dstDocJson = unmarshalledJson.get("doc");
		Array dstArrJson = unmarshalledJson.get("arr");

		Document unmarshalledText = new Document().fromJson(text);
		Document dstDocText = unmarshalledText.get("doc");
		Array dstArrText = unmarshalledText.get("arr");

//		Log.hexDump(data);

		assertEquals(unmarshalledBin, srcDoc);
		assertEquals(unmarshalledJson, srcDoc);
		assertEquals(unmarshalledText, srcDoc);

		checkTypes(dstDoc,     _byte, _short, _int, _long, _float, _double, _bool, _null, _string, _bytes, _uuid, _odt, _ld, _lt, _ldt, _arr, _doc, _bd);
		checkTypes(dstArr,     _byte, _short, _int, _long, _float, _double, _bool, _null, _string, _bytes, _uuid, _odt, _ld, _lt, _ldt, _arr, _doc, _bd);
		checkTypes(dstDocJson, _byte, _short, _int, _long, _float, _double, _bool, _null, _string, _bytes, _uuid, _odt, _ld, _lt, _ldt, _arr, _doc, _bd);
		checkTypes(dstArrJson, _byte, _short, _int, _long, _float, _double, _bool, _null, _string, _bytes, _uuid, _odt, _ld, _lt, _ldt, _arr, _doc, _bd);
		checkTypes(dstDocText, _byte, _short, _int, _long, _float, _double, _bool, _null, _string, _bytes, _uuid, _odt, _ld, _lt, _ldt, _arr, _doc, _bd);
		checkTypes(dstArrText, _byte, _short, _int, _long, _float, _double, _bool, _null, _string, _bytes, _uuid, _odt, _ld, _lt, _ldt, _arr, _doc, _bd);
	}


	private void checkTypes(Array aDstArr, Byte a_byte, Short a_short, Integer a_int, Long a_long, Float a_float, Double a_double, Boolean a_bool, Object a_null, String a_string, byte[] a_bytes, UUID a_uuid, OffsetDateTime a_odt, LocalDate a_ld, LocalTime a_lt, LocalDateTime a_ldt, Array a_arr, Document a_doc, BigDecimal a_bd)
	{
		assertEquals(aDstArr.getByte(0), a_byte);
		assertEquals(aDstArr.getShort(1), a_short);
		assertEquals(aDstArr.getInt(2), a_int);
		assertEquals(aDstArr.getLong(3), a_long);
		assertEquals(aDstArr.getFloat(4), a_float);
		assertEquals(aDstArr.getDouble(5), a_double);
		assertEquals(aDstArr.getBoolean(6), a_bool);
		assertEquals(aDstArr.get(7), a_null);
		assertEquals(aDstArr.isNull(7), true);
		assertEquals(aDstArr.getString(8), a_string);
		assertEquals(aDstArr.getBinary(9), a_bytes);
		assertEquals(aDstArr.getUUID(10), a_uuid);
		assertEquals(aDstArr.getOffsetDateTime(11), a_odt);
		assertEquals(aDstArr.getDate(12), a_ld);
		assertEquals(aDstArr.getTime(13), a_lt);
		assertEquals(aDstArr.getDateTime(14), a_ldt);
		assertEquals(aDstArr.getArray(15), a_arr);
		assertEquals(aDstArr.getDocument(16), a_doc);
		assertEquals(aDstArr.getDecimal(17), a_bd);
	}


	private void checkTypes(Document aDstDoc, Byte a_byte, Short a_short, Integer a_int, Long a_long, Float a_float, Double a_double, Boolean a_bool, Object a_null, String a_string, byte[] a_bytes, UUID a_uuid, OffsetDateTime a_odt, LocalDate a_ld, LocalTime a_lt, LocalDateTime a_ldt, Array a_arr, Document a_doc, BigDecimal a_bd)
	{
		assertEquals(aDstDoc.getByte("byte"), a_byte);
		assertEquals(aDstDoc.getShort("short"), a_short);
		assertEquals(aDstDoc.getInt("int"), a_int);
		assertEquals(aDstDoc.getLong("long"), a_long);
		assertEquals(aDstDoc.getFloat("float"), a_float);
		assertEquals(aDstDoc.getDouble("double"), a_double);
		assertEquals(aDstDoc.getBoolean("bool"), a_bool);
		assertEquals(aDstDoc.get("null"), a_null);
		assertEquals(aDstDoc.isNull("null"), true);
		assertEquals(aDstDoc.getString("string"), a_string);
		assertEquals(aDstDoc.getBinary("bytes"), a_bytes);
		assertEquals(aDstDoc.getUUID("uuid"), a_uuid);
		assertEquals(aDstDoc.getOffsetDateTime("odt"), a_odt);
		assertEquals(aDstDoc.getDate("ld"), a_ld);
		assertEquals(aDstDoc.getTime("lt"), a_lt);
		assertEquals(aDstDoc.getDateTime("ldt"), a_ldt);
		assertEquals(aDstDoc.getArray("arr"), a_arr);
		assertEquals(aDstDoc.getDocument("doc"), a_doc);
		assertEquals(aDstDoc.getDecimal("bd"), a_bd);
	}


	@Test
	public void testUnquotedJSON()
	{
		Document doc = Document.of("_id:[{$ge:20,$lt:30},test,{$exists:true},]");

		assertEquals(doc.toJson(), "{\"_id\":[{\"$ge\":20,\"$lt\":30},\"test\",{\"$exists\":true}]}");
	}


	@Test
	public void testObjectOutputStream() throws IOException, ClassNotFoundException
	{
		Document docOut1 = Document.of("_id:[{low:1,high:2}],name:'bob'");
		Document docOut2 = Document.of("_id:[{low:3,high:5}],name:'dan'");

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try (ObjectOutputStream oos = new ObjectOutputStream(baos))
		{
			oos.writeObject(docOut1);
			oos.writeUTF("hello");
			oos.writeObject(docOut2);
		}

		ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
		Object docIn1 = ois.readObject();
		String s = ois.readUTF();
		Object docIn2 = ois.readObject();

		assertEquals(docIn1, docOut1);
		assertEquals(docIn2, docOut2);
		assertEquals(s, "hello");
	}


	@Test
	public void testBinaryOutput() throws IOException, ClassNotFoundException
	{
		Document out1 = Document.of("_id:[1],name:'bob'");
		Document out2 = Document.of("_id:[2],name:'eve'");

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		out1.writeTo(baos);
		out2.writeTo(baos);

//		Log.hexDump(baos.toByteArray());

		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		Document in1 = new Document().readFrom(bais);
		Document in2 = new Document().readFrom(bais);

		assertEquals(in1, out1);
		assertEquals(in2, out2);
	}


	@Test(expectedExceptions = StreamChecksumException.class)
	public void testChecksumError() throws IOException, ClassNotFoundException
	{
		Document out = Document.of("_id:[1],name:'bob'");

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		out.writeTo(baos);

		byte[] data = baos.toByteArray();
		data[10] ^= 2; // "name" => "ncme"

//		Log.hexDump(data);

		new Document().readFrom(new ByteArrayInputStream(data));
	}


	@Test
	public void testCrossMarshalling() throws IOException, ClassNotFoundException
	{
		Document out1 = Document.of("_id:[1],name:'bob'");
		Document out2 = Document.of("_id:[2],name:'eve'");

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		baos.write(out1.toByteArray());
		baos.write('x');
		out2.writeTo(baos);

		ByteArrayInputStream input = new ByteArrayInputStream(baos.toByteArray());
		assertEquals(new Document().readFrom(input), out1);
		assertEquals(input.read(), 'x');
		assertEquals(new Document().fromByteArray(input.readAllBytes()), out2);
	}


//	@Test
//	public void testX() throws IOException, ClassNotFoundException
//	{
//		Log.hexDump(Document.of("_id:[1],name:'bob'").toByteArray());
//		Log.hexDump(Document.of("_id:[2],name:'bob'").toByteArray());
//	}


	@Test
	public void testHashcode() throws IOException, ClassNotFoundException
	{
		assertEquals(Document.of("_id:1").hashCode(), -1731609100);
		assertEquals(Document.of("_id:'1'").hashCode(), -382655104);
		assertEquals(Document.of("_id:[1]").hashCode(), 43187162);
		assertEquals(Document.of("_id:['1']").hashCode(), 1794624735);
	}


	@Test
	public void testNumericKeys() throws IOException, ClassNotFoundException
	{
		Document out = Document.of("1:1,2:1,3:1");
		byte[] data = out.toByteArray();

		Document in = new Document().fromByteArray(data);

		assertEquals(out, in);
	}
}

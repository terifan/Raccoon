package org.terifan.raccoon.document;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import org.terifan.raccoon.ObjectId;


enum BinaryType
{
	TERMINATOR(0),
	DOCUMENT(1,
		(aEncoder, aValue) -> aEncoder.writeDocument((Document)aValue),
		aDecoder -> aDecoder.readDocument(new Document())
	),
	ARRAY(2,
		(aEncoder, aValue) -> aEncoder.writeArray((Array)aValue),
		aDecoder -> aDecoder.readArray(new Array())
	),
	OBJECTID(3,
		(aEncoder, aValue) -> aEncoder.writeBytes(((ObjectId)aValue).toByteArray()),
		aDecoder -> ObjectId.fromBytes(aDecoder.readBytes(new byte[ObjectId.LENGTH]))
	),
	INT(4,
		(aEncoder, aValue) -> aEncoder.writeVarint((Integer)aValue),
		aDecoder -> (int)aDecoder.readVarint()
	),
	DOUBLE(5,
		(aEncoder, aValue) -> aEncoder.writeVarint(Long.reverseBytes(Double.doubleToLongBits((Double)aValue))),
		aDecoder -> Double.longBitsToDouble(Long.reverseBytes(aDecoder.readVarint()))
	),
	BOOLEAN(6,
		(aEncoder, aValue) -> aEncoder.writeVarint((Boolean)aValue ? 1 : 0),
		aDecoder -> aDecoder.readVarint() == 1
	),
	STRING(7,
		(aEncoder, aValue) -> aEncoder.writeString(aValue.toString()),
		aDecoder -> aDecoder.readString()
	),
	NULL(8,
		(aEncoder, aValue) -> {},
		aDecoder -> null
	),
	BYTE(9,
		(aEncoder, aValue) -> aEncoder.writeVarint(0xff & (Byte)aValue),
		aDecoder -> (byte)aDecoder.readVarint()
	),
	SHORT(10,
		(aEncoder, aValue) -> aEncoder.writeVarint((Short)aValue),
		aDecoder -> (short)aDecoder.readVarint()
	),
	LONG(11,
		(aEncoder, aValue) -> aEncoder.writeVarint((Long)aValue),
		aDecoder -> aDecoder.readVarint()
	),
	FLOAT(12,
		(aEncoder, aValue) -> aEncoder.writeVarint(Float.floatToIntBits((Float)aValue)),
		aDecoder -> Float.intBitsToFloat((int)aDecoder.readVarint())
	),
	BINARY(13,
		(aEncoder, aValue) -> aEncoder.writeBuffer((byte[])aValue),
		aDecoder -> aDecoder.readBuffer()
	),
	UUID(14,
		(aEncoder, aValue) -> aEncoder.writeVarint(((UUID)aValue).getMostSignificantBits()).writeVarint(((UUID)aValue).getLeastSignificantBits()),
		aDecoder -> new java.util.UUID(aDecoder.readVarint(), aDecoder.readVarint())
	),
	DATETIME(15,
		(aEncoder, aValue) -> aEncoder.writeUnsignedVarint(localDateToNumber(((LocalDateTime)aValue).toLocalDate())).writeUnsignedVarint(localTimeToNumber(((LocalDateTime)aValue).toLocalTime())),
		aDecoder -> LocalDateTime.of(numberToLocalDate((int)aDecoder.readUnsignedVarint()), numberToLocalTime(aDecoder.readUnsignedVarint()))
	),
	DATE(16,
		(aEncoder, aValue) -> aEncoder.writeUnsignedVarint(localDateToNumber((LocalDate)aValue)),
		aDecoder -> numberToLocalDate((int)aDecoder.readUnsignedVarint())
	),
	TIME(17,
		(aEncoder, aValue) -> aEncoder.writeUnsignedVarint(localTimeToNumber((LocalTime)aValue)),
		aDecoder -> numberToLocalTime(aDecoder.readUnsignedVarint())
	),
	OFFSETDATETIME(18,
		(aEncoder, aValue) -> aEncoder.writeUnsignedVarint(localDateToNumber(((OffsetDateTime)aValue).toLocalDate())).writeUnsignedVarint(localTimeToNumber(((OffsetDateTime)aValue).toLocalTime())).writeVarint(((OffsetDateTime)aValue).getOffset().getTotalSeconds()),
		aDecoder -> OffsetDateTime.of(numberToLocalDate((int)aDecoder.readUnsignedVarint()), numberToLocalTime(aDecoder.readUnsignedVarint()), ZoneOffset.ofTotalSeconds((int)aDecoder.readVarint()))
	),
	DECIMAL(19,
		(aEncoder, aValue) -> aEncoder.writeString(aValue.toString()),
		aDecoder -> new BigDecimal(aDecoder.readString())
	);

	public Encoder encoder;
	public Decoder decoder;


	private BinaryType(int aCode)
	{
		this(aCode, null, null);
	}


	private BinaryType(int aCode, Encoder aEncoder, Decoder aDecoder)
	{
		assert aCode == ordinal();

		encoder = aEncoder;
		decoder = aDecoder;
	}


	static BinaryType identify(Object aValue)
	{
		if (aValue == null)
		{
			return NULL;
		}

		Class<? extends Object> cls = aValue.getClass();

		if (Document.class == cls) return DOCUMENT;
		if (Array.class == cls) return ARRAY;
		if (ObjectId.class == cls) return OBJECTID;
		if (String.class == cls) return STRING;
		if (Integer.class == cls) return INT;
		if (Boolean.class == cls) return BOOLEAN;
		if (Double.class == cls) return DOUBLE;
		if (Long.class == cls) return LONG;
		if (Float.class == cls) return FLOAT;
		if (byte[].class == cls) return BINARY;
		if (BigDecimal.class == cls) return DECIMAL;
		if (UUID.class == cls) return UUID;
		if (OffsetDateTime.class == cls) return OFFSETDATETIME;
		if (LocalDateTime.class == cls) return DATETIME;
		if (LocalDate.class == cls) return DATE;
		if (LocalTime.class == cls) return TIME;
		if (Byte.class == cls) return BYTE;
		if (Short.class == cls) return SHORT;

		throw new IllegalArgumentException("Failed to marshal; unsupported value of type " + cls.getCanonicalName());
	}


	@FunctionalInterface
	static interface Encoder
	{
		void encode(BinaryEncoder aEncoder, Object aValue) throws IOException;
	}


	@FunctionalInterface
	static interface Decoder
	{
		Object decode(BinaryDecoder aDecoder) throws IOException;
	}


	private static int localDateToNumber(LocalDate aLocalDate)
	{
		return (aLocalDate.getYear() << 16) + (aLocalDate.getMonthValue() << 8) + aLocalDate.getDayOfMonth();
	}


	private static long localTimeToNumber(LocalTime aLocalTime)
	{
		return ((long)aLocalTime.getHour() << 48) + ((long)aLocalTime.getMinute() << 40) + ((long)aLocalTime.getSecond() << 32) + aLocalTime.getNano();
	}


	private static LocalDate numberToLocalDate(int aLocalDate)
	{
		try
		{
			return LocalDate.of(aLocalDate >>> 16, 0xff & (aLocalDate >>> 8), 0xff & aLocalDate);
		}
		catch (DateTimeException e)
		{
			throw new StreamException(e.getMessage());
		}
	}


	private static LocalTime numberToLocalTime(long aLocalTime)
	{
		try
		{
			return LocalTime.of((int)(aLocalTime >>> 48), (int)(0xff & (aLocalTime >>> 40)), (int)(0xff & (aLocalTime >> 32)), (int)(0xffffffffL & aLocalTime));
		}
		catch (DateTimeException e)
		{
			throw new StreamException(e.getMessage());
		}
	}
}

package org.terifan.raccoon.document;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import static org.terifan.raccoon.document.VarType.values;


enum VarType
{
	TERMINATOR(0),
	DOCUMENT(1),
	ARRAY(2),
	INT(3,
		(aOutput, aValue) -> aOutput.writeVarint((Integer)aValue),
		aInput -> (int)aInput.readVarint()
	),
	DOUBLE(4,
		(aOutput, aValue) -> aOutput.writeVarint(Long.reverseBytes(Double.doubleToLongBits((Double)aValue))),
		aInput -> Double.longBitsToDouble(Long.reverseBytes(aInput.readVarint()))
	),
	BOOLEAN(5,
		(aOutput, aValue) -> aOutput.writeVarint((Boolean)aValue ? 1 : 0),
		aInput -> aInput.readVarint() == 1
	),
	STRING(6,
		(aOutput, aValue) -> aOutput.writeString(aValue.toString()),
		aInput -> aInput.readString()
	),
	NULL(7,
		(aOutput, aValue) -> {},
		aInput -> null
	),
	BYTE(8,
		(aOutput, aValue) -> aOutput.writeVarint(0xff & (Byte)aValue),
		aInput -> (byte)aInput.readVarint()
	),
	SHORT(9,
		(aOutput, aValue) -> aOutput.writeVarint((Short)aValue),
		aInput -> (short)aInput.readVarint()
	),
	LONG(10,
		(aOutput, aValue) -> aOutput.writeVarint((Long)aValue),
		aInput -> aInput.readVarint()
	),
	FLOAT(11,
		(aOutput, aValue) -> aOutput.writeVarint(Float.floatToIntBits((Float)aValue)),
		aInput -> Float.intBitsToFloat((int)aInput.readVarint())
	),
	BINARY(12,
		(aOutput, aValue) -> aOutput.writeBuffer((byte[])aValue),
		aInput -> aInput.readBuffer()
	),
	UUID(13,
		(aOutput, aValue) -> aOutput.writeVarint(((UUID)aValue).getMostSignificantBits()).writeVarint(((UUID)aValue).getLeastSignificantBits()),
		aInput -> new java.util.UUID(aInput.readVarint(), aInput.readVarint())
	),
	DATETIME(14,
		(aOutput, aValue) -> aOutput.writeUnsignedVarint(localDateToNumber(((LocalDateTime)aValue).toLocalDate())).writeUnsignedVarint(localTimeToNumber(((LocalDateTime)aValue).toLocalTime())),
		aInput -> LocalDateTime.of(numberToLocalDate((int)aInput.readUnsignedVarint()), numberToLocalTime(aInput.readUnsignedVarint()))
	),
	DATE(15,
		(aOutput, aValue) -> aOutput.writeUnsignedVarint(localDateToNumber((LocalDate)aValue)),
		aInput -> numberToLocalDate((int)aInput.readUnsignedVarint())
	),
	TIME(16,
		(aOutput, aValue) -> aOutput.writeUnsignedVarint(localTimeToNumber((LocalTime)aValue)),
		aInput -> numberToLocalTime(aInput.readUnsignedVarint())
	),
	OFFSETDATETIME(17,
		(aOutput, aValue) -> aOutput.writeUnsignedVarint(localDateToNumber(((OffsetDateTime)aValue).toLocalDate())).writeUnsignedVarint(localTimeToNumber(((OffsetDateTime)aValue).toLocalTime())).writeVarint(((OffsetDateTime)aValue).getOffset().getTotalSeconds()),
		aInput -> OffsetDateTime.of(numberToLocalDate((int)aInput.readUnsignedVarint()), numberToLocalTime(aInput.readUnsignedVarint()), ZoneOffset.ofTotalSeconds((int)aInput.readVarint()))
	);

	public final int code;
	public Encoder encoder;
	public Decoder decoder;


	private VarType(int aCode)
	{
		code = aCode;
	}


	private VarType(int aCode, Encoder aEncoder, Decoder aDecoder)
	{
		code = aCode;
		encoder = aEncoder;
		decoder = aDecoder;
	}


	public static VarType get(int aCode)
	{
		VarType type = values()[aCode];
		assert type.ordinal() == aCode;
		return type;
	}


	static VarType identify(Object aValue)
	{
		if (aValue == null)
		{
			return NULL;
		}

		Class<? extends Object> cls = aValue.getClass();

		if (Document.class == cls) return DOCUMENT;
		if (Array.class == cls) return ARRAY;
		if (String.class == cls) return STRING;
		if (Integer.class == cls) return INT;
		if (Boolean.class == cls) return BOOLEAN;
		if (Double.class == cls) return DOUBLE;
		if (Long.class == cls) return LONG;
		if (Float.class == cls) return FLOAT;
		if (Byte.class == cls) return BYTE;
		if (Short.class == cls) return SHORT;
		if (LocalDate.class == cls) return DATE;
		if (LocalTime.class == cls) return TIME;
		if (LocalDateTime.class == cls) return DATETIME;
		if (OffsetDateTime.class == cls) return OFFSETDATETIME;
		if (UUID.class == cls) return UUID;
		if (byte[].class == cls) return BINARY;

		throw new IllegalArgumentException("Failed to marshal an unsupported value of type " + cls.getCanonicalName());
	}


	@FunctionalInterface
	static interface Encoder
	{
		void encode(VarOutputStream aOutput, Object aValue) throws IOException;
	}


	@FunctionalInterface
	static interface Decoder
	{
		Object decode(VarInputStream aInput) throws IOException;
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
		return LocalDate.of(aLocalDate >>> 16, 0xff & (aLocalDate >>> 8), 0xff & aLocalDate);
	}


	private static LocalTime numberToLocalTime(long aLocalTime)
	{
		return LocalTime.of((int)(aLocalTime >>> 48), (int)(0xff & (aLocalTime >>> 40)), (int)(0xff & (aLocalTime >> 32)), (int)(0xffffffffL & aLocalTime));
	}
}

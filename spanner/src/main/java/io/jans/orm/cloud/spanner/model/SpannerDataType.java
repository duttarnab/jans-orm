package io.jans.orm.cloud.spanner.model;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.Code;

public enum SpannerDataType {
	BOOL {
		@Override
		public int getSqlType() {
			return Types.BOOLEAN;
		}

		@Override
		public Class<Boolean> getJavaClass() {
			return Boolean.class;
		}

		@Override
		public Code getCode() {
			return Code.BOOL;
		}

		@Override
		public List<Boolean> getArrayElements(ResultSet rs, int columnIndex) {
			return rs.getBooleanList(columnIndex);
		}

		@Override
		public Type getGoogleType() {
			return Type.bool();
		}
	},
	DATE {
		@Override
		public int getSqlType() {
			return Types.DATE;
		}

		@Override
		public Class<Date> getJavaClass() {
			return Date.class;
		}

		@Override
		public Code getCode() {
			return Code.DATE;
		}

		@Override
		public List<Date> getArrayElements(ResultSet rs, int columnIndex) {
			return toJavaDates(rs.getDateList(columnIndex));
		}

		public List<Date> toJavaDates(List<com.google.cloud.Date> dates) {
			List<Date> res = new ArrayList<>(dates.size());
			for (com.google.cloud.Date date : dates) {
				res.add(com.google.cloud.Date.toJavaUtilDate(date));
			}

			return res;
		}

		@Override
		public Type getGoogleType() {
			return Type.date();
		}
	},
	FLOAT64 {
		private Set<Class<?>> classes = new HashSet<>(Arrays.asList(BigDecimal.class, Float.class, Double.class));

		@Override
		public int getSqlType() {
			return Types.DOUBLE;
		}

		@Override
		public Class<Double> getJavaClass() {
			return Double.class;
		}

		@Override
		public Set<Class<?>> getSupportedJavaClasses() {
			return classes;
		}

		@Override
		public Code getCode() {
			return Code.FLOAT64;
		}

		@Override
		public List<Double> getArrayElements(ResultSet rs, int columnIndex) {
			return rs.getDoubleList(columnIndex);
		}

		@Override
		public Type getGoogleType() {
			return Type.float64();
		}
	},
	INT64 {
		private Set<Class<?>> classes = new HashSet<>(Arrays.asList(Byte.class, Integer.class, Long.class));

		@Override
		public int getSqlType() {
			return Types.BIGINT;
		}

		@Override
		public Class<Long> getJavaClass() {
			return Long.class;
		}

		@Override
		public Set<Class<?>> getSupportedJavaClasses() {
			return classes;
		}

		@Override
		public Code getCode() {
			return Code.INT64;
		}

		@Override
		public List<Long> getArrayElements(ResultSet rs, int columnIndex) {
			return rs.getLongList(columnIndex);
		}

		@Override
		public Type getGoogleType() {
			return Type.int64();
		}
	},
	STRING {
		@Override
		public int getSqlType() {
			return Types.NVARCHAR;
		}

		@Override
		public Class<String> getJavaClass() {
			return String.class;
		}

		@Override
		public Code getCode() {
			return Code.STRING;
		}

		@Override
		public List<String> getArrayElements(ResultSet rs, int columnIndex) {
			return rs.getStringList(columnIndex);
		}

		@Override
		public Type getGoogleType() {
			return Type.string();
		}
	},
	TIMESTAMP {
		@Override
		public int getSqlType() {
			return Types.TIMESTAMP;
		}

		@Override
		public Class<Timestamp> getJavaClass() {
			return Timestamp.class;
		}

		@Override
		public Code getCode() {
			return Code.TIMESTAMP;
		}

		@Override
		public List<Date> getArrayElements(ResultSet rs, int columnIndex) {
			return toJavaDates(rs.getTimestampList(columnIndex));
		}
		
		

		public List<Date> toJavaDates(List<com.google.cloud.Timestamp> dates) {
			List<Date> res = new ArrayList<>(dates.size());
			for (com.google.cloud.Timestamp date : dates) {
				res.add(new java.util.Date(date.toSqlTimestamp().getTime()));
			}

			return res;
		}

		@Override
		public Type getGoogleType() {
			return Type.timestamp();
		}
	};

	public abstract int getSqlType();

	public abstract Code getCode();

	public abstract Type getGoogleType();

	/**
	 * 
	 * @param rs          the result set to look up the elements
	 * @param columnIndex zero based column index
	 * @return The corresponding array elements of the type in the given result set
	 */
	public abstract List<?> getArrayElements(ResultSet rs, int columnIndex);

	public String getTypeName() {
		return name();
	}

	public abstract Class<?> getJavaClass();

	public Set<Class<?>> getSupportedJavaClasses() {
		return Collections.singleton(getJavaClass());
	}

	public static SpannerDataType getType(Class<?> clazz) {
		for (SpannerDataType type : SpannerDataType.values()) {
			if (type.getSupportedJavaClasses().contains(clazz))
				return type;
		}
		return null;
	}

	public static SpannerDataType getType(Code code) {
		for (SpannerDataType type : SpannerDataType.values()) {
			if (type.getCode() == code)
				return type;
		}
		return null;
	}

}

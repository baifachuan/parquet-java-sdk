package com.fcbai.parquet.sdk;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Optional;

import static java.util.Optional.of;

public class CustomSimpleRecordConverter extends GroupConverter {
    private final Converter converters[];
    private final String name;
    private final CustomSimpleRecordConverter parent;
    protected CustomSimpleRecord record;

    public CustomSimpleRecordConverter(GroupType schema) {
        this(schema, null, null);
    }

    public CustomSimpleRecordConverter(GroupType schema, String name, CustomSimpleRecordConverter parent) {
        this.converters = new Converter[schema.getFieldCount()];
        this.parent = parent;
        this.name = name;

        int i = 0;
        for (Type field: schema.getFields()) {
            converters[i++] = createConverter(field);
        }
    }

    private Converter createConverter(Type field) {
        LogicalTypeAnnotation ltype = field.getLogicalTypeAnnotation();

        if (field.isPrimitive()) {
            if (ltype != null) {
                return ltype.accept(new LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<Converter>() {
                    @Override
                    public Optional<Converter> visit(LogicalTypeAnnotation.StringLogicalTypeAnnotation stringLogicalType) {
                        return of(new CustomSimpleRecordConverter.StringConverter(field.getName()));
                    }

                    @Override
                    public Optional<Converter> visit(LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalType) {
                        int scale = decimalLogicalType.getScale();
                        return of(new CustomSimpleRecordConverter.DecimalConverter(field.getName(), scale));
                    }
                }).orElse(new CustomSimpleRecordConverter.SimplePrimitiveConverter(field.getName()));
            }
            return new CustomSimpleRecordConverter.SimplePrimitiveConverter(field.getName());
        }

        GroupType groupType = field.asGroupType();
        if (ltype != null) {
            return ltype.accept(new LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<Converter>() {
                @Override
                public Optional<Converter> visit(LogicalTypeAnnotation.MapLogicalTypeAnnotation mapLogicalType) {
                    return of(new CustomSimpleMapRecordConverter(groupType, field.getName(), CustomSimpleRecordConverter.this));
                }

                @Override
                public Optional<Converter> visit(LogicalTypeAnnotation.ListLogicalTypeAnnotation listLogicalType) {
                    return of(new CustomSimpleListRecordConverter(groupType, field.getName(), CustomSimpleRecordConverter.this));
                }
            }).orElse(new CustomSimpleRecordConverter(groupType, field.getName(), this));
        }
        return new CustomSimpleRecordConverter(groupType, field.getName(), this);
    }

    @Override
    public Converter getConverter(int fieldIndex) {
        return converters[fieldIndex];
    }

    CustomSimpleRecord getCurrentRecord() {
        return record;
    }

    @Override
    public void start() {
        record = new CustomSimpleRecord();
    }

    @Override
    public void end() {
        if (parent != null) {
            parent.getCurrentRecord().add(name, record);
        }
    }

    private class SimplePrimitiveConverter extends PrimitiveConverter {
        protected final String name;

        public SimplePrimitiveConverter(String name) {
            this.name = name;
        }

        @Override
        public void addBinary(Binary value) {
            record.add(name, value.getBytes());
        }

        @Override
        public void addBoolean(boolean value) {
            record.add(name, value);
        }

        @Override
        public void addDouble(double value) {
            record.add(name, value);
        }

        @Override
        public void addFloat(float value) {
            record.add(name, value);
        }

        @Override
        public void addInt(int value) {
            record.add(name, value);
        }

        @Override
        public void addLong(long value) {
            record.add(name, value);
        }
    }

    private class StringConverter extends CustomSimpleRecordConverter.SimplePrimitiveConverter {
        public StringConverter(String name) {
            super(name);
        }

        @Override
        public void addBinary(Binary value) {
            record.add(name, value.toStringUsingUTF8());
        }
    }

    private class DecimalConverter extends CustomSimpleRecordConverter.SimplePrimitiveConverter {
        private final int scale;

        public DecimalConverter(String name, int scale) {
            super(name);
            this.scale = scale;
        }

        @Override
        public void addBinary(Binary value) {
            record.add(name, new BigDecimal(new BigInteger(value.getBytes()), scale));
        }

        @Override
        public void addInt(int value) {
            record.add(name, BigDecimal.valueOf(value).movePointLeft(scale));
        }

        @Override
        public void addLong(long value) {
            record.add(name, BigDecimal.valueOf(value).movePointLeft(scale));
        }
    }
}

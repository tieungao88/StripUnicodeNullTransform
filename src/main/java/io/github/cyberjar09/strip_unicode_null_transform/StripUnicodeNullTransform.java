package io.github.cyberjar09.strip_unicode_null_transform;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Map;

public class StripUnicodeNullTransform<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final String FIELDS_CONFIG = "fields";
    private static final String DEFAULT_FIELDS = "*";
    private static final String DEBUG = "debug";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELDS_CONFIG, ConfigDef.Type.STRING, DEFAULT_FIELDS, ConfigDef.Importance.MEDIUM, "Comma-separated list of fields to strip null characters from (default: all fields)")
            .define(DEBUG, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.LOW, "Show debug output of records");

    private String[] fields;
    private boolean isDebugModeEnabled;

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        isDebugModeEnabled = config.getBoolean(DEBUG);
        String fieldsString = config.getString(FIELDS_CONFIG);
        if (fieldsString == null || fieldsString.isEmpty() || fieldsString.equals(DEFAULT_FIELDS)) {
            fields = null; // Include all fields
        } else {
            fields = fieldsString.split(",");
        }
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void close() {
        // No resources to release
    }

    @Override
    public R apply(R record) {
        printRecordForDebugging(record, "inputRecord");

        if (record.value() instanceof Struct) {
            Struct struct = (Struct) record.value();
            for (Field field : struct.schema().fields()) {
                if (fields == null || contains(fields, field.name())) {
                    Object value = struct.get(field);
                    if (value instanceof String) {
                        // https://stackoverflow.com/a/28990116/1310021
                        String newValue = ((String) value).replace("\\u0000", "").replace("\u0000", "");
                        struct.put(field, newValue);
                    }
                }
            }
            printRecordForDebugging(record, "newRecord");
            return record;
        } else {
            throw new DataException("StripUnicodeNullTransform only works with Structs, but found " + record.getClass());
        }
    }

    private boolean contains(String[] array, String value) {
        for (String element : array) {
            if (element.equals(value)) {
                return true;
            }
        }
        return false;
    }

    private void printRecordForDebugging(R record, String recordName) {
        if (isDebugModeEnabled) {
            System.out.println("-------");
            System.out.println(recordName + " >>> " + record);
            System.out.println(recordName + ".valueSchema.fields >>> " + record.valueSchema().fields());
            record.valueSchema().fields().forEach(f -> {
                System.out.println(">>> Field: " + f.name() + " -> isOptional:" + f.schema().isOptional());
            });
            System.out.println(recordName + ".valueSchema.name >>> " + record.valueSchema().name());
            System.out.println(recordName + ".valueSchema.isOptional >>> " + record.valueSchema().isOptional());
            System.out.println(recordName + ".valueSchema.defaultValue >>> " + record.valueSchema().defaultValue());
            System.out.println(recordName + ".valueSchema.version >>> " + record.valueSchema().version());
            System.out.println(recordName + ".valueSchema.doc >>> " + record.valueSchema().doc());
            System.out.println(recordName + ".valueSchema.parameters >>> " + record.valueSchema().parameters());
            System.out.println("-------");
        }
    }

}

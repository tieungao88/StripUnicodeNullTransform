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

    private String[] fields;

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(configDef(), configs);
        String fieldsString = config.getString(FIELDS_CONFIG);
        if (fieldsString == null || fieldsString.isEmpty() || fieldsString.equals(DEFAULT_FIELDS)) {
            fields = null; // Include all fields
        } else {
            fields = fieldsString.split(",");
        }
    }

    @Override
    public R apply(R record) {
        if (record.value() instanceof Struct) {
            Struct struct = (Struct) record.value();
            for (Field field : struct.schema().fields()) {
                if (fields == null || contains(fields, field.name())) {
                    Object value = struct.get(field);
                    if (value instanceof String) {
                        String newValue = ((String) value).replaceAll("\u0000", "");
                        struct.put(field, newValue);
                    }
                }
            }
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

    @Override
    public ConfigDef config() {
        return new ConfigDef()
                .define(FIELDS_CONFIG, ConfigDef.Type.STRING, DEFAULT_FIELDS,
                        ConfigDef.Importance.MEDIUM, "Comma-separated list of fields to strip null characters from (default: all fields)");
    }

    public ConfigDef configDef() {
        return config();
    }

    @Override
    public void close() {
        // No resources to release
    }
}

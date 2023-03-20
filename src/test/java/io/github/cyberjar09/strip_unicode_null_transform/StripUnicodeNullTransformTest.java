package io.github.cyberjar09.strip_unicode_null_transform;


import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

public class StripUnicodeNullTransformTest {
    @Test
    public void testTransformWithAllFields() {
        StripUnicodeNullTransform<SinkRecord> transform = new StripUnicodeNullTransform<>();
        transform.configure(new HashMap<String, String>());

        Schema schema = SchemaBuilder.struct()
                .field("field1", Schema.STRING_SCHEMA)
                .field("field2", Schema.STRING_SCHEMA)
                .field("field3", Schema.STRING_SCHEMA)
                .build();

        Struct inputValue = new Struct(schema)
                .put("field1", "foo\u0000bar")
                .put("field2", "baz")
                .put("field3", "qux\u0000quux");

        Struct expectedValue = new Struct(schema)
                .put("field1", "foobar")
                .put("field2", "baz")
                .put("field3", "quxquux");

        SinkRecord output = transform.apply(new SinkRecord("", 0, null, null, schema, inputValue, 0));

        assertEquals(expectedValue, output.value());
    }

    @Test
    public void testTransformWithSelectedFields() {
        Map<String, String> config = new HashMap<>();
        config.put("fields", "field1,field3");

        StripUnicodeNullTransform<SinkRecord> transform = new StripUnicodeNullTransform<>();
        transform.configure(config);

        Schema schema = SchemaBuilder.struct()
                .field("field1", Schema.STRING_SCHEMA)
                .field("field2", Schema.STRING_SCHEMA)
                .field("field3", Schema.STRING_SCHEMA)
                .field("field4", Schema.STRING_SCHEMA)
                .build();

        Struct inputValue = new Struct(schema)
                .put("field1", "foo\u0000bar")
                .put("field2", "baz")
                .put("field3", "qux\u0000quux")
                .put("field4", "a\u0000b");

        Struct expectedValue = new Struct(schema)
                .put("field1", "foobar")
                .put("field2", "baz")
                .put("field3", "quxquux")
                .put("field4", "a\u0000b");

        SinkRecord output = transform.apply(new SinkRecord("", 0, null, null, schema, inputValue, 0));

        assertEquals(expectedValue, output.value());
    }
}
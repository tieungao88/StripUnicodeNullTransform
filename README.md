# README

custom Single Message Transform that replaces all occurrences of the Unicode character `\u0000` (or its equivalent in hexadecimal, `\0x00`) with nothing in the specified fields of a Struct record

This implementation takes a configuration parameter called `fields` which is a comma-separated list of field names to strip null characters from. If no value is provided, then all fields will be processed.

To use this transform in a Kafka Connect sink connector, you can specify the transforms and `transforms.stripNullChars.type` properties in the connector configuration file, like so:

```
transforms=stripNullChars
transforms.stripNullChars.type=io.github.cyberjar09.strip_unicode_null_transform.StripUnicodeNullTransform
transforms.stripNullChars.fields=field1,field2
```
In this example, the transform will only strip null characters from the `field1` and `field2` fields of the Struct record. 
If the fields property is not specified or is set to the default value of *, then all fields will be processed.
Regex is not currently supported (PR welcome)

Note that you will need to package the `StripUnicodeNullTransform` class and its dependencies into a JAR file and add it to the Kafka Connect worker's classpath.

### To create JAR
`mvn clean install`
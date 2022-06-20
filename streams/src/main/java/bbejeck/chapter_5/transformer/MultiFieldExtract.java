package bbejeck.chapter_5.transformer;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

/**
 * User: Bill Bejeck
 * Date: 6/19/22
 * Time: 2:48 PM
 */
public abstract class MultiFieldExtract<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String EXTRACT_FIELDS_CONFIG = "extract.fields";
   

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(EXTRACT_FIELDS_CONFIG, ConfigDef.Type.LIST, ConfigDef.Importance.HIGH,
                    "List of field names to extract for the new connectRecord");

    private static final String PURPOSE = "extracting";
    private List<String> fieldNamesToExtract;

    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        fieldNamesToExtract = config.getList(EXTRACT_FIELDS_CONFIG);
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
    }

    @Override
    public R apply(R connectRecord) {
        if (operatingValue(connectRecord) == null) {
            return connectRecord;
        } else if (operatingSchema(connectRecord) == null) {
            return applySchemaless(connectRecord);
        } else {
            return applyWithSchema(connectRecord);
        }
    }

    @Override
    public void close() {
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    protected abstract Schema operatingSchema(R connectRecord);

    protected abstract Object operatingValue(R connectRecord);

    protected abstract R newRecord(R connectRecord, Schema updatedSchema, Object updatedValue);

    private R applySchemaless(R connectRecord) {
        final Map<String, Object> originalRecord = requireMap(operatingValue(connectRecord), PURPOSE);
        final Map<String, Object> newRecord = new LinkedHashMap<>();
        List<Map.Entry<String,Object>> filteredEntryList = originalRecord.entrySet().stream().filter(entry -> fieldNamesToExtract.contains(entry.getKey())).toList();
        filteredEntryList.forEach(entry -> newRecord.put(entry.getKey(), entry.getValue()));
        return newRecord(connectRecord, null, newRecord);
    }

    private R applyWithSchema(R connectRecord) {
        final Struct value = requireStruct(operatingValue(connectRecord), PURPOSE);

        Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if(updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(value.schema());
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }
        final Struct updatedValue = new Struct(updatedSchema);

        for (Field field : updatedValue.schema().fields()) {
            updatedValue.put(field.name(), value.get(field.name()));
        }
       return newRecord(connectRecord, updatedSchema, updatedValue);
    }



    private Schema makeUpdatedSchema(Schema schema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        List<Field> filteredFields = schema.fields().stream().filter(fld -> fieldNamesToExtract.contains(fld.name())).toList();
        filteredFields.forEach(field -> builder.field(field.name(), field.schema()));
        return builder.build();
    }

    public static class Key<R extends ConnectRecord<R>> extends MultiFieldExtract<R> {
        @Override
        protected Schema operatingSchema(R connectRecord) {
            return connectRecord.keySchema();
        }

        @Override
        protected Object operatingValue(R connectRecord) {
            return connectRecord.key();
        }

        @Override
        protected R newRecord(R connectRecord, Schema updatedSchema, Object updatedValue) {
            return connectRecord.newRecord(connectRecord.topic(), connectRecord.kafkaPartition(), updatedSchema, updatedValue, connectRecord.valueSchema(), connectRecord.value(), connectRecord.timestamp());
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends MultiFieldExtract<R> {
        @Override
        protected Schema operatingSchema(R connectRecord) {
            return connectRecord.valueSchema();
        }

        @Override
        protected Object operatingValue(R connectRecord) {
            return connectRecord.value();
        }

        @Override
        protected R newRecord(R connectRecord, Schema updatedSchema, Object updatedValue) {
            return connectRecord.newRecord(connectRecord.topic(), connectRecord.kafkaPartition(), connectRecord.keySchema(), connectRecord.key(), updatedSchema, updatedValue, connectRecord.timestamp());
        }
    }


}

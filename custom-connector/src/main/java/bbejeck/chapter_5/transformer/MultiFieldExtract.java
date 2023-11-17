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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

/**
 * Multi field extractor that pulls designated fields from a JSON payload
 */
public abstract class MultiFieldExtract<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger LOG = LoggerFactory.getLogger(MultiFieldExtract.class);
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
        LOG.debug("Transforming connect record {}", connectRecord);
        if (operatingValue(connectRecord) == null) {
            LOG.debug("Operating value is null returning null");
            return connectRecord;
        } else if (operatingSchema(connectRecord) == null) {
            LOG.debug("No operating schema");
            return applySchemaless(connectRecord);
        } else {
            LOG.debug("Applying transform with schema");
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
        List<Map.Entry<String,Object>> filteredEntryList = originalRecord.entrySet().stream().filter(entry -> fieldNamesToExtract.contains(entry.getKey())).collect(Collectors.toList());
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

        updatedValue.schema().fields().forEach(field -> updatedValue.put(field.name(), value.get(field.name())));
       return newRecord(connectRecord, updatedSchema, updatedValue);
    }



    private Schema makeUpdatedSchema(Schema schema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        List<Field> filteredFields = schema.fields().stream().filter(fld -> fieldNamesToExtract.contains(fld.name())).collect(Collectors.toList());
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
            LOG.debug("Value schema is {}", connectRecord.valueSchema());
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

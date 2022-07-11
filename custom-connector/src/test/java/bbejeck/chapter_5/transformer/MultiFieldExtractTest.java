package bbejeck.chapter_5.transformer;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNull.nullValue;

class MultiFieldExtractTest {

    MultiFieldExtract<SourceRecord> multiFieldExtract;
    Map<String, String> configuration = Map.of(MultiFieldExtract.EXTRACT_FIELDS_CONFIG, "bid,ask,displayName,symbol");


    @BeforeEach
    void setUp() {
        multiFieldExtract = new MultiFieldExtract.Value<>();
        multiFieldExtract.configure(configuration);
    }

    @Test
    @DisplayName("Transforming with a schema")
    void shouldTransformSchema() {

        final Schema startingSchema = SchemaBuilder.struct().name("stock-api").version(1).doc("yahoo-api")
                .field("quoteType", Schema.STRING_SCHEMA)
                .field("typeDisp", Schema.STRING_SCHEMA)
                .field("triggerable", Schema.BOOLEAN_SCHEMA)
                .field("sharesOutstanding", Schema.INT32_SCHEMA)
                .field("bookValue", Schema.FLOAT64_SCHEMA)
                .field("fiftyDayAverage", Schema.FLOAT64_SCHEMA)
                .field("displayName", Schema.STRING_SCHEMA)
                .field("symbol", Schema.STRING_SCHEMA)
                .field("bid", Schema.FLOAT64_SCHEMA)
                .field("ask", Schema.FLOAT64_SCHEMA).build();

        final Schema expectedSchema = SchemaBuilder.struct().name("stock-api").version(1).doc("yahoo-api")
                .field("displayName", Schema.STRING_SCHEMA)
                .field("symbol", Schema.STRING_SCHEMA)
                .field("bid", Schema.FLOAT64_SCHEMA)
                .field("ask", Schema.FLOAT64_SCHEMA).build();


        final Struct startingStruct = new Struct(startingSchema)
                .put("quoteType", "EQUITY")
                .put("typeDisp", "Equity")
                .put("triggerable", true)
                .put("sharesOutstanding", 125926000)
                .put("bookValue", 3.007)
                .put("fiftyDayAverage", 27.1522)
                .put("displayName", "Confluent")
                .put("symbol", "CFLT")
                .put("bid", 333.33)
                .put("ask", 222.56);

        final SourceRecord record = new SourceRecord(null, null, "test", 0, startingSchema, startingStruct);
        final SourceRecord transformedRecord = multiFieldExtract.apply(record);


        assertThat(startingSchema.name(), equalTo(transformedRecord.valueSchema().name()));
        assertThat(startingSchema.version(), equalTo(transformedRecord.valueSchema().version()));
        assertThat(startingSchema.doc(), equalTo(transformedRecord.valueSchema().doc()));

        assertThat(transformedRecord.valueSchema(), equalTo(expectedSchema));

        assertThat(((Struct) transformedRecord.value()).getFloat64("bid"), equalTo(333.33));
        assertThat(((Struct) transformedRecord.value()).getFloat64("ask"), equalTo(222.56));
        assertThat(((Struct) transformedRecord.value()).getString("symbol"), equalTo("CFLT"));
        assertThat(((Struct) transformedRecord.value()).getString("displayName"), equalTo("Confluent"));


        // Test the caching of the schema
        final SourceRecord transformedRecord2 = multiFieldExtract.apply(
                new SourceRecord(null, null, "test", 1, startingSchema, startingStruct));

        assertThat(transformedRecord.valueSchema(), sameInstance(transformedRecord2.valueSchema()));
    }

    @Test
    @DisplayName("Transforming without a schema")
    void shouldTransformNoSchema() {

        final Map<String, Object> incomingValue = new HashMap<>();

        incomingValue.put("quoteType", "EQUITY");
        incomingValue.put("typeDisp", "Equity");
        incomingValue.put("triggerable", true);
        incomingValue.put("sharesOutstanding", 125926000);
        incomingValue.put("bookValue", 3.007);
        incomingValue.put("fiftyDayAverage", 27.1522);
        incomingValue.put("displayName", "Confluent");
        incomingValue.put("symbol", "CFLT");
        incomingValue.put("bid", 333.33);
        incomingValue.put("ask", 222.56);

        final SourceRecord record = new SourceRecord(null, null, "test", 0,
                null, incomingValue);

        final SourceRecord transformedRecord = multiFieldExtract.apply(record);
        assertThat(((Map<?, ?>) transformedRecord.value()).get("bid"), equalTo(333.33));
        assertThat(((Map<?, ?>) transformedRecord.value()).get("ask"), equalTo(222.56));
        assertThat(((Map<?, ?>) transformedRecord.value()).get("symbol"), equalTo("CFLT"));
        assertThat(((Map<?, ?>) transformedRecord.value()).get("displayName"), equalTo("Confluent"));
    }

    @Test
    @DisplayName("Transforming should handle a SourceRecord with a null value")
    void shouldTransformNullValue() {
        final SourceRecord record = new SourceRecord(null, null, "test", 0,
                null, null);

        final SourceRecord transformedRecord = multiFieldExtract.apply(record);
        assertThat(transformedRecord.value(), nullValue());
    }

    @AfterEach
    void tearDown() {
        multiFieldExtract.close();
    }
}
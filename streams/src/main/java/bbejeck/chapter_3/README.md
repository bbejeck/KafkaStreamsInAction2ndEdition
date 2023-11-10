The code in chapter 3 is used for demonstrating how to interact with Schema
Registry and not with an emphasis on the producer and consumer code.

I've broken up the examples across two main packages: `bbejeck.chapter_3.producer` and `bbejeck.chapter_3.consumer`
Within those two, there are additional packages of `avro`, `proto`, and `json` containing
example code used to drive serializing and deserializing examples using the
serializers for the given format indicated by the package name.
### A guided tour to the chapter 3 code
It's a good idea for me to describe the contents of the directories and the function of
each class:
* bbejeck.chapter_3.producer
    * avro
        * `AvroProducer` Initial producer example for working with Avro schemas
        * `AvroReferenceCollegeProducer` Example producer for working with schema references in Avro
        * `AvroReferenceCompanyProducer` Example producer for working with schema references in Avro
    * json
        * `JsonSchemaProducer` Initial producer example for working with JsonSchema schemas
        * `JsonSchemaReferenceCollegeProducer` Example producer for working with schema references in JsonSchema
        * `JsonScheamReferenceCompanyProducer` Example producer for working with schema references in JsonSchema
    * proto
        * `ProtoProducer` Initial producer example for working with Protobuf schemas
        * `ProtoReferenceCollegeProducer` Example producer for working with schema references in Protobuf
        * `ProtoReferenceCompanyProducer` Example producer for working with schema references in Protobuf

* bbejeck.chapter_3.consumer
    * avro
        * `AvroConsumer` Initial consumer example for working with Avro schemas
        * `AvroReferenceCollegeConsumer` Example consumer for working with schema references in Avro
        * `AvroReferenceCompanyConsumer` Example consumer for working with schema references in Avro
    * json
        * `JsonSchemaConsumer` Initial consumer example for working with JsonSchema schemas
        * `JsonSchemaReferenceCollegeConsumer` Example consumer for working with schema references in JsonSchema
        * `JsonScheamReferenceCompanyConsumer` Example consumer for working with schema references in JsonSchema
    * proto
        * `ProtoConsumer` Initial consumer example for working with Protobuf schemas
        * `ProtoReferenceCollegeConsumer` Example consumer for working with schema references in Protobuf
        * `ProtoReferenceCompanyConsumer` Example consumer for working with schema references in Protobuf

* bbejeck.chapter_3
    *  `AvroReflectionProduceConsumeExample` A simple example using the AvroReflection serializer and deserializer.

### Running the examples

For the examples nested under the `producer` or `consumer` packages, you need run them in steps:
1. Run the producer - the producer examples send a few records then shutdown
2. Run the consumer - the consumer starts up and displays some information on the console then it shuts down
   after two consecutive `poll` calls without retrieving any records it shuts down.

There are tests that your can run instead of the producer-consumer steps. In the
`src/test/java/bbejeck/chapter_3` package there are three tests for the Avro, Protobuf and JsonSchema
producer-consumer interaction with SchemaRegistry.  You can still run the examples as stand-alone
programs if you wish, but should you choose to experiment you'll be able to run tests to ensure everything still works as
expected.
package bbejeck.chapter_3.codegen;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kjetland.jackson.jsonSchema.JsonSchemaGenerator;

public class JsonSchemaGen {

    public static void main(String[] args) {
        Class<?> theClass;
        if (args.length == 0) {
            System.out.println("Must include the class name");
            System.exit(1);
        }

        try {
            theClass = Class.forName(args[0]);
            ObjectMapper mapper = new ObjectMapper();
            JsonSchemaGenerator schemaGenerator = new JsonSchemaGenerator(mapper);
            JsonNode jsonNode = schemaGenerator.generateJsonSchema(theClass);
            String schema = mapper.writerWithDefaultPrettyPrinter()
                    .writeValueAsString(jsonNode);
            System.out.println(schema);

        } catch (ClassNotFoundException | JsonProcessingException e) {
            System.out.println("Problem generating the schema");
            e.printStackTrace();
        }
    }
}

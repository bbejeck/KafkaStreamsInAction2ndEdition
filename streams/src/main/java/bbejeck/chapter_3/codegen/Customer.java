package bbejeck.chapter_3.codegen;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaString;

/**
 * Basic Java POJO for JsonSchema examples
 */
@JsonSchemaInject(strings=
        {@JsonSchemaString(
                path="javaType",
                value="bbejeck.chapter_3.codegen.Customer")})
public class Customer {
    @JsonProperty
    private String name;
    @JsonProperty
    private int id;
    @JsonProperty
    private String email;

    public Customer() {
    }

    public Customer(String name, int id, String email) {
        this.name = name;
        this.id = id;
        this.email = email;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }
}

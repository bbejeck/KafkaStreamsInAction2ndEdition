package bbejeck.spring.java;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * User: Bill Bejeck
 * Date: 10/11/22
 * Time: 6:23 PM
 */
public class LoanApplicationProcessingApplication {

    public static void main(String[] args) {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext("bbejeck.spring.application",
                "bbejeck.spring.java", "bbejeck.spring.datagen");
    }
}

package io.epoxi.repository.api.model;

import io.swagger.jaxrs.config.BeanConfig;
import io.swagger.models.Swagger;
import io.swagger.models.auth.BasicAuthDefinition;
import io.swagger.util.Yaml;
import org.junit.jupiter.api.Test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class SwaggerTest {

    @Test
    void generateYaml()
    {
        assertDoesNotThrow(SwaggerTest::generateOpenAPIDoc, "Generated");
    }

    public static void generateOpenAPIDoc()
    {
        BeanConfig beanConfig = new BeanConfig();
        beanConfig.setVersion("1.0.2");
        beanConfig.setSchemes(new String[]{"http"});
        beanConfig.setHost("localhost:8002");
        beanConfig.setBasePath("/api");
        beanConfig.setResourcePackage("io.epoxi.repository.api.model");
        beanConfig.setScan(true);

        Swagger swagger = beanConfig.getSwagger();
        swagger.addSecurityDefinition("basicAuth", new BasicAuthDefinition());
        beanConfig.configure(swagger);
        beanConfig.scanAndRead();
        try
        {
            String yaml = Yaml.mapper().writeValueAsString(swagger);
            String fileName = "D:\\Dev\\epoxi\\app\\repository-api\\src\\main\\resources\\epoxi-repository-api.yaml";
            writeToFile(fileName, yaml);
        }
        catch (Exception exception)
        {
            System.out.println("Cannot generate OpenAPI doc");
        }
    }

    private static void writeToFile(String fileName, String yaml)
    {

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName)))
        {
            writer.write(yaml);
        }
        catch(IOException ex)
        {
            System.out.println("Cannot generate OpenAPI doc");
        }

    }

}

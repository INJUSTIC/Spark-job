package org.example;
import cloud.ilum.job.Job;
import org.apache.spark.sql.*;
import scala.Option;
import scala.Some;
import scala.collection.immutable.Map;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class InteractiveJobExample implements Job {
    @Override
    public Option<String> run(SparkSession sparkSession, Map<String, Object> config) {
        String userParam = config.getOrElse("userParam", () -> "None");

        String[] data = {"Alice", "Bob", "Charlie", "David"};

        Dataset<Row> df = sparkSession.createDataset(Arrays.asList(data), Encoders.STRING()).toDF("name");

        df.createOrReplaceTempView("people");

        Dataset<Row> result = sparkSession.sql("SELECT name, length(name) as name_length FROM people");

        List<String> resultStrings = result.collectAsList().stream()
                .map(Row::toString)
                .collect(Collectors.toList());

        String resultString = String.join("\n", resultStrings);
        return Some.apply("Hello " + userParam + "! Spark SQL query executed. Result:\n" + resultString);
        /*String userParam = config.getOrElse("userParam", () -> "None");

        copyResourceToWorkingDirectory("people.json");

        Dataset<Row> peopleDF = sparkSession.read().format("json")
                .load("people.json");

        peopleDF.createOrReplaceTempView("people");

        Dataset<Row> result1 = sparkSession.sql("SELECT * FROM people WHERE age > 25");
        Dataset<Row> result2 = sparkSession.sql("SELECT gender, AVG(age) as avg_age FROM people GROUP BY gender");

        System.out.println("Result 1:");
        result1.show();

        System.out.println("Result 2:");
        result2.show();

        String resultString = "Hello " + userParam + "! Spark SQL queries executed.\n\n" +
                "Result 1:\n" + result1.collectAsList().toString() + "\n\n" +
                "Result 2:\n" + result2.collectAsList().toString();

        return Some.apply(resultString);*/
    }

   /* private void copyResourceToWorkingDirectory(String resourceName) {
        try {
            URL resourceUrl = getClass().getClassLoader().getResource(resourceName);

            if (resourceUrl != null) {
                Path sourcePath;

                if (resourceUrl.getProtocol().equals("jar")) {
                    FileSystem fileSystem = FileSystems.newFileSystem(resourceUrl.toURI(), Collections.emptyMap());
                    sourcePath = fileSystem.getPath(resourceName);
                } else {
                    sourcePath = Paths.get(resourceUrl.toURI());
                }

                Path targetPath = new File("people.json").toPath();

                Files.copy(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
            } else {
                throw new RuntimeException("Resource not found: " + resourceName);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error copying resource to working directory.", e);
        }
    }*/
}

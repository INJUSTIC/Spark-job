package org.example;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class Main {
    public static void main(String[] args) {
        generateJsonData("src/main/resources/people.json", 100);
    }

    private static void generateJsonData(String filePath, int numRows) {
        try (FileWriter writer = new FileWriter(filePath)) {
            writer.write("[");

            Random random = new Random();
            String[] genders = {"Male", "Female"};

            for (int i = 0; i < numRows; i++) {
                String name = "Person" + i;
                int age = random.nextInt(50) + 20;
                String gender = genders[random.nextInt(2)];

                writer.write("{\"name\":\"" + name + "\",\"age\":" + age + ",\"gender\":\"" + gender + "\"}");

                if (i < numRows - 1) {
                    writer.write(",");
                }
            }

            writer.write("]");
        } catch (IOException e) {
            throw new RuntimeException("Error generating JSON data.", e);
        }
    }
}
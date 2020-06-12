package com.tbc.batchtask;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class BatchTaskApplication {
    public static void main(String[] args) throws ClassNotFoundException {
        String aClass = null;
        for (int i = 0; i < args.length; i++) {
            if ("--mclass".equals(args[i])) {
                 aClass = args[i + 1];
            }
        }
        System.out.println(aClass);
        SpringApplication app = new SpringApplication(Class.forName(aClass));
        app.run(args);
    }

}

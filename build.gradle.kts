// See https://gradle.org and https://github.com/gradle/kotlin-dsl

// Apply the java plugin to add support for Java
plugins {
    java
    application
}

repositories {
    jcenter()
}

dependencies {
    // Annotations for better code documentation
    compile("com.intellij:annotations:12.0")

    // JUnit Jupiter test framework
    testCompile("org.junit.jupiter:junit-jupiter-api:5.3.1")


    // HTTP client for unit tests
    testCompile("org.apache.httpcomponents:fluent-hc:4.5.3")

    // Apache commons
    compile("org.apache.commons:commons-io:1.3.2")

    // One-nio server
    compile("ru.odnoklassniki:one-nio:1.0.1")

    // Logging
    compile("org.apache.logging.log4j:log4j-core:2.11.1")

}

tasks {
    "test"(Test::class) {
        maxHeapSize = "128m"
    }
}

application {
    // Define the main class for the application
    mainClassName = "ru.mail.polis.Server"

    // And limit Xmx
    applicationDefaultJvmArgs = listOf("-Xmx128m")
}

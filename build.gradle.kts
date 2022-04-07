group = "rationalenterprise"
description = "Media diff tool for comparing components and load files"
version = "0.1.0"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

plugins {
    application
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("commons-codec:commons-codec:1.15")
    implementation("info.picocli:picocli:4.6.2")
    implementation("org.apache.commons:commons-text:1.9")
}

application {
    mainClass.set("com.rationalenterprise.mediadiff.MediaDiffApplication")
}

tasks.jar {
    manifest {
        attributes(mapOf("Main-Class" to "com.rationalenterprise.mediadiff.MediaDiffApplication",
						 "Implementation-Title" to project.name,
                         "Implementation-Version" to project.version))
    }

    exclude("LICENSE.txt")
    exclude("META-INF/LICENSE")
    exclude("META-INF/LICENSE.txt")
    exclude("NOTICE.txt")
    exclude("META-INF/NOTICE")
    exclude("META-INF/NOTICE.txt")

    // This is used to make the jar fat.
    from(configurations.runtimeClasspath.get().map({ if (it.isDirectory) it else zipTree(it) }))
}
group = "rationalenterprise"
description = "Media diff tool for comparing components and load files"
version = "0.1.0"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(11))
    }
}

plugins {
    application
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("info.picocli:picocli:4.6.2")
    implementation("commons-codec:commons-codec:1.15")
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

    from(configurations.runtimeClasspath.get().map({ if (it.isDirectory) it else zipTree(it) }))
}

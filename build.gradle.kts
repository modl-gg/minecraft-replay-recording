plugins {
    `java-library`
    `maven-publish`
}

val packetEventsVersion = "2.12.3"

group = "gg.modl.minecraft.replay"
version = "1.1.2"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(8))
    }
}

repositories {
    mavenCentral()
    maven("https://nexus.modl.gg/repository/maven-releases/")
    maven("https://nexus.modl.gg/repository/maven-snapshots/")
    maven("https://repo.codemc.io/repository/maven-releases/")
    maven("https://repo.codemc.io/repository/maven-snapshots/")
}

dependencies {
    api("gg.modl.minecraft.replay:replay-format:1.1.0")
    implementation("com.google.code.gson:gson:2.11.0")

    // Platform-agnostic PacketEvents API from the Modl fork to match server/runtime behavior.
    compileOnly("gg.modl.minecraft.packetevents:packetevents-api:$packetEventsVersion")
    compileOnly("net.kyori:adventure-api:4.26.1")

    compileOnly("org.projectlombok:lombok:1.18.24")
    annotationProcessor("org.projectlombok:lombok:1.18.24")

    testImplementation("gg.modl.minecraft.packetevents:packetevents-api:$packetEventsVersion")
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
}

tasks.test {
    useJUnitPlatform()
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
        }
    }
    repositories {
        maven {
            name = "ModlNexus"
            url = uri("https://nexus.modl.gg/repository/maven-releases/")
            credentials {
                username = System.getenv("NEXUS_USER") ?: findProperty("nexus.user") as String?
                password = System.getenv("NEXUS_PASS") ?: findProperty("nexus.pass") as String?
            }
        }
    }
}

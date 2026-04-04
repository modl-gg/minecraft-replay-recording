plugins {
    `java-library`
    `maven-publish`
}

group = "gg.modl.replay"
version = "1.1.0"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(8))
    }
}

repositories {
    mavenCentral()
    maven("https://nexus.modl.gg/repository/maven-releases/")
    maven("https://repo.codemc.io/repository/maven-releases/")
    maven("https://repo.codemc.io/repository/maven-snapshots/")
}

dependencies {
    api("gg.modl.replay:replay-format:1.1.0")
    implementation("com.google.code.gson:gson:2.11.0")

    // Platform-agnostic PacketEvents API — works with Spigot, Fabric, and NeoForge backends
    compileOnly("com.github.retrooper:packetevents-api:2.11.2")
    compileOnly("net.kyori:adventure-api:4.26.1")

    compileOnly("org.projectlombok:lombok:1.18.24")
    annotationProcessor("org.projectlombok:lombok:1.18.24")
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

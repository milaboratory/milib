import com.palantir.gradle.gitversion.VersionDetails
import groovy.lang.Closure

plugins {
    `java-library`
    `java-test-fixtures`
    `maven-publish`
    id("io.github.gradle-nexus.publish-plugin") version "1.1.0"
    id("com.palantir.git-version") version "0.12.3"
}

val miRepoAccessKeyId: String by project
val miRepoSecretAccessKey: String by project

val versionDetails: Closure<VersionDetails> by extra
val gitDetails = versionDetails()

val longTests: String? by project

group = "com.milaboratory"
version =
    if (gitDetails.commitDistance == 0) gitDetails.lastTag
    else "${gitDetails.lastTag}-${gitDetails.commitDistance}-${gitDetails.gitHash}"
description = "MiLib"

java.sourceCompatibility = JavaVersion.VERSION_1_8

tasks.register("createInfoFile") {
    doLast {
        projectDir
            .resolve("build_info.json")
            .writeText("""{"version":"$version"}""")
    }
}

repositories {
    mavenCentral()
}

dependencies {
    api("org.apache.commons:commons-math3:3.6.1")
    api("cc.redberry:pipe:1.0.0-alpha0")

    implementation("com.fasterxml.jackson.core:jackson-databind:2.11.4")
    implementation("org.apache.commons:commons-compress:1.20")
    implementation("commons-io:commons-io:2.7")
    implementation("org.lz4:lz4-java:1.7.1")
    implementation("com.beust:jcommander:1.72")
    implementation("info.picocli:picocli:4.1.2")
    implementation("net.sf.trove4j:trove4j:3.0.3")

    testFixturesImplementation("com.fasterxml.jackson.core:jackson-databind:2.11.4")
    testFixturesImplementation("junit:junit:4.13.2")

    testImplementation("junit:junit:4.13.2")
    testImplementation("org.mockito:mockito-all:1.10.19")
}

java.sourceCompatibility = JavaVersion.VERSION_1_8

publishing {
    repositories {
        maven {
            name = "mipub"
            url = uri("s3://milaboratory-artefacts-public-files.s3.eu-central-1.amazonaws.com/maven")

            authentication {
                credentials(AwsCredentials::class) {
                    accessKey = miRepoAccessKeyId
                    secretKey = miRepoSecretAccessKey
                }
            }
        }
    }

    publications.create<MavenPublication>("maven") {
        from(components["java"])
    }
}

tasks.withType<JavaCompile>() {
    options.encoding = "UTF-8"
}

tasks.test {
    useJUnit()
    minHeapSize = "1024m"
    maxHeapSize = "2048m"

    longTests?.let { systemProperty("longTests", it) }
}

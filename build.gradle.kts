import com.palantir.gradle.gitversion.VersionDetails
import groovy.lang.Closure

plugins {
    java
    `maven-publish`
    id("io.github.gradle-nexus.publish-plugin") version "1.1.0"
    id("com.palantir.git-version") version "0.12.3"
}

val miRepoAccessKeyId: String by project
val miRepoSecretAccessKey: String by project

val versionDetails: Closure<VersionDetails> by extra
val gitDetails = versionDetails()

group = "com.milaboratory"
version =
    if (gitDetails.commitDistance == 0) gitDetails.lastTag
    else "${gitDetails.lastTag}-${gitDetails.commitDistance}-${gitDetails.gitHash}"
description = "MiLib"

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
    implementation("cc.redberry:pipe:1.0.0-alpha0")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.11.4")
    implementation("org.apache.commons:commons-compress:1.20")
    implementation("org.apache.commons:commons-math3:3.6.1")
    implementation("commons-io:commons-io:2.7")
    implementation("org.lz4:lz4-java:1.7.1")
    implementation("com.beust:jcommander:1.72")
    implementation("info.picocli:picocli:4.1.2")
    implementation("net.sf.trove4j:trove4j:3.0.3")
    testImplementation("junit:junit:4.13.2")
    testImplementation("org.mockito:mockito-all:1.10.19")
}

java.sourceCompatibility = JavaVersion.VERSION_1_8

val testsJar by tasks.registering(Jar::class) {
    archiveClassifier.set("tests")
    from(sourceSets.test.get().output)
}

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
        artifact(testsJar)
    }
}

tasks.withType<JavaCompile>() {
    options.encoding = "UTF-8"
}

tasks.test {
    useJUnit()
}

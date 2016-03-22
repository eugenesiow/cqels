# CQELS (Continuous Query Evaluation over Linked Data)

This repository is a fork of <https://github.com/KMax/cqels> for the purpose of testing CQELS performance with [SRBench](https://www.w3.org/wiki/SRBench) on lightweight computers. 
It was orginially forked from the CQELS respository on [Google Code](https://code.google.com/p/cqels/).

## Running SRBench

Add the following repository to your pom.xml:
```
<repository>
    <id>cqels.mvn-repo</id>
    <url>https://raw.github.com/KMax/cqels/mvn-repo/</url>
    <snapshots>
        <enabled>true</enabled>
        <updatePolicy>always</updatePolicy>
    </snapshots>
</repository>
```

and declare the following dependency:
```
<dependency>
    <groupId>org.deri.cqels</groupId>
    <artifactId>cqels</artifactId>
    <version>...</version>
</dependency>
```

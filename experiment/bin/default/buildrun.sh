#!/bin/bash
gradle clean

gradle fatJar

java -jar -Xmx32g build/libs/experiment-all-0.1.jar

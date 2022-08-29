Cameron Brightwell  - Cameron.Brightwell@rams.colostate.edu
Kevin Conner        - Kevin.Conner@colostate.edu
Mridul Banik        - Mridul.Banik@colostate.edu

CS455 - Distributed Systems
Spring 2022
Team 14

Homework 3: Analytics Using Hadoop

Usage:
    Questions 1-5:
        hadoop fs -put data_sets/aqi/parsed_aqi_data.csv /input
        hadoop jar build/libs/CS455-Homework-3-1.0-SNAPSHOT.jar cs455.aqi.Q[n] /input/parsed_aqi_data.csv /output
    Question 6:
        hadoop fs -put data_sets/aqi/parsed_aqi_data_q6.csv /input
        hadoop fs -put data_sets/oil_refineries/parsed_oil_refineries_data.csv /input
        hadoop jar build/libs/CS455-Homework-3-1.0-SNAPSHOT.jar cs455.aqi.Q6 /input/parsed_aqi_data_q6.csv /input/parsed_oil_refineries_data.csv /output

Building:
    This project uses gradle: gradle assemble

File Manifest:
    /build/libs/CS455-Homework-3.jar
        - The final, compiled jar file.
    /gradle
        - Gradle's wrapper directory.
    /src
        - src directory of the Java files, see the Java Manifest section.
    build.gradle
        - The gradle build file.
    README.txt
        - This file, detailing the project.

Java Manifest:
    cs455.aqi
        DateUtility.java
            - Converting epoch time to other formats.
        Q1_Mapper.java
        Q1_Reducer.java
        Q1.java
            - The best and worst Days of Week for AQI scores.
        Q2_Mapper.java
        Q2_Reducer.java
        Q2.java
            - The best and worst Months for AQI scores.
        Q3_Mapper.java
        Q3_Reducer.java
        Q3.java
            - Best 10 average AQI scores for the year 2020.
        Q4_Mapper.java
        Q4_Reducer.java
        Q4.java
            - Best 10 average AQI scores for the year 2020.
        Q5_Mapper.java
        Q5_Reducer.java
        Q5.java
            - Biggest one week change for each county.
        Q6_Aqi_Mapper.java
        Q6_nRefineries_Mapper.java
        Q6_Reducer.java
        Q6.java
            - Gets rank based on state and total AQI. Gets rank based on number of refineries in state. 
            - Add these two ranks for an overall rank score.
            

        

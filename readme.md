# Project 1 - IGDB API with Spark/Hive

## Project Description

This project utilizes Spark SQL and Hive in order to analyze data retrieved from an API. Here I connected to the IGDB (Internet Game Database) API in order to get information on various games corresponding to user input. This information is written to a json file, and then stored in a table. Using this table filled with data I ran several queries on it using Hive and Spark. 

## Technologies Used 

* Scala 2.11.12
* Spark 2.3.0

## Features

Features currently implemented:
* Access IGDB API
* Write data to JSON file
* Perform queries on data using HiveQL
* Basic user account functionality

Future features:
* Improved account functionality including hashed passwords
* Improved queries taking more user input
* Pagination support for API data, allowing us to use larger data sets
* Create an executable file 

## Getting Started 

Clone the repository using the following command in the terminal: 
git clone https://github.com/AlexWhite252/project1.git

Ensure you have the appropriate version of Spark installed

Open project in an IDE and run it

## Usage

When you run the project you'll be prompted for account information. 
Once you create an account or login to your account you'll be prompted to enter a year. 
After entering a year the program will run through the rest of its code and exit. 


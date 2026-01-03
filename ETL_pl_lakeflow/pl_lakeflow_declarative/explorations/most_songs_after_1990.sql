-- Databricks notebook source
USE CATALOG `workspace`;
USE SCHEMA `getting_started`;

-- COMMAND ----------

-- Which artists released the most songs each year in 1990 or later?
SELECT artist_name, total_number_of_songs, year
  -- replace with the catalog/schema you are using:
  FROM top_artists_by_year
  WHERE year >= 1990
  ORDER BY total_number_of_songs DESC, year DESC;

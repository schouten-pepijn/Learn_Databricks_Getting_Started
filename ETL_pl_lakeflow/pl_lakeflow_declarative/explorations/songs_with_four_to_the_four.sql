-- Databricks notebook source
USE CATALOG workspace;
USE SCHEMA getting_started;

-- COMMAND ----------

-- Find songs with a 4/4 beat and danceable tempo
SELECT artist_name, song_title, tempo
  -- replace with the catalog/schema you are using:
  FROM songs_prepared
  WHERE time_signature = 4 AND tempo between 100 and 140;

# Flink State Benchmark

Ce projet Apache Flink a pour but de tester et comparer les performances des traitements de données avec état (`StatefulMaxBidFilterJob`) et sans état (`StatelessBidFilterJob`).

## Prérequis
* Java (8 ou 11 recommandé)
* Apache Maven
* Un cluster Apache Flink (local ou distant)

## Compilation
Pour compiler le code et générer le fichier exécutable :

```bash
mvn clean package

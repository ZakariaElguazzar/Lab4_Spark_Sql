# Rapport TP Spark SQL: Analyse du Système de Location de Vélos

**Big Data 2025**

**Rapport de Travaux Pratiques**

**Étudiant:** [Votre Nom]  
**Groupe:** [Votre Groupe]  
**Date:** 2025-12-07

**Encadré par:** M. Abdelmajid BOUSSELHAM  
**Institution:** [Votre École/Université]

---

## Table des Matières

1. [Introduction](#introduction)
2. [Contexte et Objectifs](#contexte)
3. [Environnement Technique](#environnement)
4. [Description des Données](#donnees)
5. [Démarche Suivie](#demarche)
6. [Requêtes et Résultats](#requetes)
7. [Résultats Détaillés](#resultats)
8. [Interprétation des Résultats](#interpretation)
9. [Difficultés Rencontrées et Solutions](#difficultes)
10. [Conclusion](#conclusion)
11. [Annexes](#annexes)

---

## 1. Introduction <a name="introduction"></a>

Ce rapport présente les travaux pratiques réalisés sur **Spark SQL** dans le cadre de l'analyse d'un système de location de vélos en libre-service. L'objectif principal était de manipuler un jeu de données transactionnel à l'aide de requêtes SQL exécutées dans un environnement Spark, déployé localement et en cluster HDFS.

L'analyse couvre plusieurs aspects du système :
- Comportement des utilisateurs
- Popularité des stations
- Analyse temporelle (heures de pointe)
- Performance globale du système

## 2. Contexte et Objectifs <a name="contexte"></a>

### Contexte
Une ville souhaite analyser l'utilisation de son système de vélos en libre-service pour optimiser les ressources, améliorer le service aux usagers et planifier les maintenances.

### Objectifs
Les objectifs techniques étaient :
1. Charger et explorer les données avec Spark DataFrame
2. Créer une vue temporaire SQL
3. Exécuter des requêtes analytiques complexes
4. Déployer l'application sur un cluster Spark/HDFS
5. Visualiser et interpréter les résultats

## 3. Environnement Technique <a name="environnement"></a>

### Technologies Utilisées

| Technologie | Version |
|------------|---------|
| Apache Spark | 4.0.0 |
| Hadoop HDFS | 3.3.6 |
| Java | 21 |
| Maven | 3.9+ |
| Docker | Latest |
| Docker Compose | v2 |

### Architecture Déployée
L'infrastructure complète a été conteneurisée avec Docker Compose :

```yaml
services:
  namenode:
    image: apache/hadoop:3.3.6
    ports: ["9870:9870", "8020:8020"]
  
  datanode:
    image: apache/hadoop:3.3.6
  
  resourcemanager:
    image: apache/hadoop:3.3.6
    ports: ["8088:8088"]
  
  nodemanager:
    image: apache/hadoop:3.3.6
  
  spark-master:
    image: spark:latest
    ports: ["7077:7077", "8080:8080"]
  
  spark-worker-1:
    image: spark:latest
```

## 4. Description des Données <a name="donnees"></a>

### Fichier Source
- **Nom:** `bike_sharing.csv`
- **Lignes:** 100 locations
- **Colonnes:** 10 attributs

### Structure des Données

| Colonne | Description |
|---------|-------------|
| rental_id | Identifiant unique de location |
| user_id | Identifiant unique de l'utilisateur |
| age | Âge de l'utilisateur |
| gender | Genre (M/F) |
| start_time | Heure de début (timestamp) |
| end_time | Heure de fin (timestamp) |
| start_station | Station de départ |
| end_station | Station d'arrivée |
| duration_minutes | Durée en minutes |
| price | Prix en dollars |

## 5. Démarche Suivie <a name="demarche"></a>

### Préparation du Projet Maven

```xml
<dependencies>
    <!-- Spark Core -->
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-core_2.13</artifactId>
        <version>${spark.version}</version>
    </dependency>
    
    <!-- Spark SQL -->
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-sql_2.13</artifactId>
        <version>${spark.version}</version>
    </dependency>
</dependencies>
```

### Code Principal - Application Local

```java
import org.apache.spark.sql.*;

public class BikeSharingAppLocal {
    public static void main(String[] args) {
        // Initialisation Spark
        SparkSession spark = SparkSession.builder()
                .appName("BikeSharingAnalysis")
                .master("local[*]")
                .getOrCreate();
        
        // Chargement CSV
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("bike_sharing.csv");
        
        // Exploration
        df.printSchema();
        df.show(5);
        System.out.println("Number of rentals: " + df.count());
        
        // Création vue SQL
        df.createOrReplaceTempView("bike_rentals_view");
        
        // Requête 1: Locations > 30 minutes
        Dataset<Row> longRentals = spark.sql(
            "SELECT * FROM bike_rentals_view WHERE duration_minutes > 30"
        );
        
        spark.stop();
    }
}
```

### Code Principal - Application HDFS

```java
import org.apache.spark.sql.*;

public class BikeSharingAppHdfs {
    public static void main(String[] args) {
        // Configuration HDFS
        SparkConf conf = new SparkConf()
                .setAppName("BikeSharingAnalysisHDFS")
                .set("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020");
        
        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        
        // Chargement depuis HDFS
        String hdfsPath = "hdfs://namenode:8020/bike_sharing.csv";
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(hdfsPath);
        
        // Exécution des requêtes...
        spark.stop();
    }
}
```

### Commandes d'Exécution

```bash
# 1. Compilation et packaging
mvn clean compile package

# 2. Lancement de l'infrastructure
docker compose up -d

# 3. Copie des fichiers
docker cp target/Lab4_spark_sql-1.0-SNAPSHOT.jar spark-master:/opt/spark/
docker cp bike_sharing.csv namenode:/data

# 4. Transfert vers HDFS
docker exec -it namenode bash
hdfs dfs -put /data/bike_sharing.csv /

# 5. Exécution Spark
/opt/spark/bin/spark-submit --master spark://spark-master:7077 Lab4_spark_sql-1.0-SNAPSHOT.jar
```

## 6. Requêtes et Résultats <a name="requetes"></a>

### Exploration Initiale

```sql
-- Schema
df.printSchema();

-- Premières lignes
df.show(5);

-- Nombre total
SELECT COUNT(*) FROM bike_rentals_view;
-- Résultat: 100 locations
```

### Requêtes de Base

```sql
-- 1. Locations > 30 minutes
SELECT * FROM bike_rentals_view 
WHERE duration_minutes > 30;
-- Résultat: 58 locations

-- 2. Locations depuis Station A
SELECT * FROM bike_rentals_view 
WHERE start_station = 'Station A';
-- Résultat: 17 locations

-- 3. Revenu total
SELECT SUM(price) AS total_revenue 
FROM bike_rentals_view;
-- Résultat: 871.0 $
```

### Requêtes d'Agrégation

```sql
-- 1. Locations par station
SELECT start_station, COUNT(*) AS rental_count
FROM bike_rentals_view
GROUP BY start_station
ORDER BY rental_count DESC;

-- 2. Durée moyenne par station
SELECT start_station, AVG(duration_minutes) AS avg_duration
FROM bike_rentals_view
GROUP BY start_station;

-- 3. Station la plus utilisée
SELECT start_station, COUNT(*) AS rental_count
FROM bike_rentals_view
GROUP BY start_station
ORDER BY rental_count DESC
LIMIT 1;
-- Résultat: Station B (27 locations)
```

### Analyse Temporelle

```sql
-- 1. Extraction de l'heure
SELECT *, HOUR(start_time) AS rental_hour
FROM bike_rentals_view;

-- 2. Locations par heure
SELECT HOUR(start_time) AS rental_hour, 
       COUNT(*) AS rental_count
FROM bike_rentals_view
GROUP BY rental_hour
ORDER BY rental_hour;

-- 3. Station populaire le matin (7h-12h)
SELECT start_station, COUNT(*) AS rental_count
FROM bike_rentals_view
WHERE HOUR(start_time) BETWEEN 7 AND 12
GROUP BY start_station
ORDER BY rental_count DESC
LIMIT 1;
-- Résultat: Station E (8 locations)
```

### Analyse des Utilisateurs

```sql
-- 1. Âge moyen
SELECT AVG(age) AS average_age 
FROM bike_rentals_view;
-- Résultat: 41.91 ans

-- 2. Répartition par genre
SELECT gender, COUNT(*) AS user_count
FROM bike_rentals_view
GROUP BY gender;

-- 3. Groupe d'âge le plus actif
SELECT 
    CASE 
        WHEN age BETWEEN 18 AND 30 THEN '18-30'
        WHEN age BETWEEN 31 AND 40 THEN '31-40'
        WHEN age BETWEEN 41 AND 50 THEN '41-50'
        WHEN age >= 51 THEN '51+'
    END AS age_group,
    COUNT(*) AS rental_count
FROM bike_rentals_view
GROUP BY age_group
ORDER BY rental_count DESC
LIMIT 1;
-- Résultat: 51+ (34 locations)
```

## 7. Résultats Détaillés <a name="resultats"></a>

### Tableau Synthétique des Résultats

| Métrique | Description | Valeur |
|----------|-------------|--------|
| Locations totales | Nombre de transactions | 100 |
| Revenu total | Somme des prix | 871.0 $ |
| Durée moyenne | Moyenne générale | 34.75 min |
| Âge moyen | Moyenne des âges | 41.91 ans |
| **Station la plus utilisée** | Station B | 27 locations |
| **Station matinale** | Station E (7h-12h) | 8 locations |
| **Pic horaire** | 17h | 7 locations |
| **Groupe d'âge** | 51+ ans | 34 locations |

### Répartition par Station

| Station | Nombre de Locations | Durée Moyenne (min) |
|---------|-------------------|-------------------|
| Station A | 17 | 31.94 |
| Station B | 27 | 36.74 |
| Station C | 14 | 33.00 |
| Station D | 19 | 33.79 |
| Station E | 23 | 36.74 |

### Répartition par Groupe d'Âge

| Groupe d'Âge | Nombre de Locations |
|--------------|-------------------|
| 18-30 ans | 24 |
| 31-40 ans | 21 |
| 41-50 ans | 21 |
| 51+ ans | 34 |

## 8. Interprétation des Résultats <a name="interpretation"></a>

### Popularité des Stations
- **Station B** est la plus fréquentée globalement (27 locations)
- La **Station E** domine le créneau matinal (7h-12h)
- Les durées moyennes sont similaires sur toutes les stations (32-37 min)

### Comportement Temporel
- **Pic d'utilisation**: 17h (7 locations)
- **Période calme**: Nuit (0h-6h) et après-midi tardif
- Distribution relativement uniforme sur la journée

### Profil des Utilisateurs
- **Âge moyen**: 41.91 ans
- **Groupe dominant**: 51+ ans (34% des locations)
- **Répartition genre**: Presque équilibrée (52% F, 48% M)

### Implications Opérationnelles
1. **Répartition des vélos**: Renforcer Station B et Station E
2. **Maintenance**: Programmer pendant les heures creuses
3. **Marketing**: Cibler les 50+ ans et les trajets matinaux
4. **Tarification**: Possibilité de modulation horaire

## 9. Difficultés Rencontrées et Solutions <a name="difficultes"></a>

### Problème 1: Connexion HDFS
- **Symptôme**: Spark ne trouve pas le fichier sur HDFS
- **Cause**: Configuration incorrecte de `fs.defaultFS`
- **Solution**:
```java
SparkConf conf = new SparkConf()
    .set("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020");
```

### Problème 2: Packaging des Dépendances
- **Symptôme**: ClassNotFoundException à l'exécution
- **Cause**: Dépendances Spark non incluses dans le JAR
- **Solution**: Utilisation de `maven-assembly-plugin`
```xml
<plugin>
    <artifactId>maven-assembly-plugin</artifactId>
    <configuration>
        <descriptorRefs>
            <descriptorRef>jar-with-dependencies</descriptorRef>
        </descriptorRefs>
    </configuration>
</plugin>
```

### Problème 3: Persistance HDFS
- **Symptôme**: Données perdues au redémarrage
- **Cause**: Volumes Docker non persistants
- **Solution**: Montage de volumes locaux
```yaml
volumes:
  - ./volumes/namenode/name:/hadoop/dfs/name
  - ./volumes/datanode/data:/hadoop/dfs/data
```

## 10. Conclusion <a name="conclusion"></a>

Ce TP a permis de maîtriser plusieurs aspects essentiels du Big Data avec Spark :

### Acquis Techniques
- Déploiement d'un cluster Spark/HDFS conteneurisé
- Programmation Spark SQL en Java
- Analyse exploratoire de données transactionnelles
- Packaging et déploiement d'applications Spark

### Acquis Analytiques
- Identification des patterns d'utilisation
- Analyse temporelle et segmentation utilisateur
- Production d'indicateurs opérationnels
- Recommandations basées sur les données

### Perspectives
1. **Étendre l'analyse**: Ajouter des données météo, événements
2. **Prédiction**: Modèles ML pour prévoir la demande
3. **Temps réel**: Dashboard de monitoring
4. **Optimisation**: Algorithme de répartition dynamique

## 11. Annexes <a name="annexes"></a>

### Structure du Projet
```
Lab4_spark_sql/
├── src/main/java/
│   ├── BikeSharingAppLocal.java
│   └── BikeSharingAppHdfs.java
├── pom.xml
├── docker-compose.yaml
├── bike_sharing.csv
└── target/
    └── Lab4_spark_sql-1.0-SNAPSHOT.jar
```

### Résultats d'Exécution
```
Schema:
root
 |-- rental_id: integer
 |-- user_id: integer
 |-- age: integer
 |-- gender: string
 |-- start_time: timestamp
 |-- end_time: timestamp
 |-- start_station: string
 |-- end_station: string
 |-- duration_minutes: integer
 |-- price: double

Nombre de locations: 100
Revenu total: 871.0
Station la plus utilisée: Station B (27 locations)
Groupe d'âge dominant: 51+ (34 locations)
```

### Commandes Utiles pour le Monitoring
```bash
# Vérification des services
docker ps

# Logs Spark Master
docker logs spark-master

# Interface Web HDFS
http://localhost:9870

# Interface Web Spark
http://localhost:8080

# Interface Web YARN
http://localhost:8088

# Liste fichiers HDFS
docker exec namenode hdfs dfs -ls /
```

## Bibliographie

- Apache Spark Documentation. *Spark SQL and DataFrames*. 2024. https://spark.apache.org/docs/latest/sql-programming-guide.html
- Apache Hadoop Documentation. *HDFS Architecture*. 2024. https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html
- Docker Documentation. *Docker Compose*. 2024. https://docs.docker.com/compose/

---

*Rédigé et exécuté par:*  
[Votre Nom]  
Étudiant en Big Data  
2025-12-07

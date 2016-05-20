# recommender

## build mvn package
```
mvn package
```

## run from local
```
spark-submit --class com.cloudera.datascience.recommender.RunRecommender --master local --driver-memory 6g <jarfile>.jar <data folder>
```

## run from spark on marathon

# spark-window-testing
Playing around with Spark Window Functions

// Create some data to test
// Goal is to calculate GB-seconds and VCore-seconds from a polling data set which 
// samples resource usage by user *roughly* every 30 seconds  
val sampledUsage = Seq(
    ("wade", 10, 4, 1496976678.1),
    ("wade", 10, 4, 1496976709.8),
    ("wade", 10, 4, 1496976738.3),
    ("wade", 10, 4, 1496976768.8),
    ("wade", 10, 4, 1496976798.1),
    ("wade", 10, 4, 1496976828.8),
    ("wade", 10, 4, 1496976858.5),
    ("wade", 10, 4, 1496976888.8),
    ("john", 1, 10, 1496976678.6),
    ("john", 2, 20, 1496976704.8),
    ("john", 3, 30, 1496976738.2),
    ("john", 4, 40, 1496976768.8),
    ("john", 5, 50, 1496976798.8),
    ("john", 7, 70, 1496976858.8),
    ("john", 8, 80, 1496976888.3)
).toDF("name", "cpus", "gb", "timestamp")

df.show()


sampledUsage: org.apache.spark.sql.DataFrame = [name: string, cpus: int ... 2 more fields]
+----+----+---+--------------+
|name|cpus| gb|     timestamp|
+----+----+---+--------------+
|wade|  10|  4|1.4969766781E9|
|wade|  10|  4|1.4969767098E9|
|wade|  10|  4|1.4969767383E9|
|wade|  10|  4|1.4969767688E9|
|wade|  10|  4|1.4969767981E9|
|wade|  10|  4|1.4969768288E9|
|wade|  10|  4|1.4969768585E9|
|wade|  10|  4|1.4969768888E9|
|john|   1| 10|1.4969766786E9|
|john|   2| 20|1.4969767048E9|
|john|   3| 30|1.4969767382E9|
|john|   4| 40|1.4969767688E9|
|john|   5| 50|1.4969767988E9|
|john|   7| 70|1.4969768588E9|
|john|   8| 80|1.4969768883E9|
+----+----+---+--------------+

// Import the window functions.
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

val wspec = Window.partitionBy("name").orderBy($"timestamp".asc)
val  interpolatedUsage = df.withColumn("timestampPrev", lag( $"timestamp", 1 ).over(wspec))
                           .withColumn("GBsecs", ($"timestamp" - $"timestampPrev") * $"gb" )
                           .withColumn("VCoresecs", ($"timestamp" - $"timestampPrev") * $"cpus" )
interpolatedUsage.show()

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
wspec: org.apache.spark.sql.expressions.WindowSpec = org.apache.spark.sql.expressions.WindowSpec@5ae644e0
interpolatedUsage: org.apache.spark.sql.DataFrame = [name: string, cpus: int ... 5 more fields]
+----+----+---+--------------+--------------+------------------+------------------+
|name|cpus| gb|     timestamp| timestampPrev|            GBsecs|         VCoresecs|
+----+----+---+--------------+--------------+------------------+------------------+
|john|   1| 10|1.4969766786E9|          null|              null|              null|
|john|   2| 20|1.4969767048E9|1.4969766786E9| 524.0000009536743| 52.40000009536743|
|john|   3| 30|1.4969767382E9|1.4969767048E9| 1002.000002861023| 100.2000002861023|
|john|   4| 40|1.4969767688E9|1.4969767382E9|1223.9999961853027|122.39999961853027|
|john|   5| 50|1.4969767988E9|1.4969767688E9|            1500.0|             150.0|
|john|   7| 70|1.4969768588E9|1.4969767988E9|            4200.0|             420.0|
|john|   8| 80|1.4969768883E9|1.4969768588E9|            2360.0|             236.0|
|wade|  10|  4|1.4969766781E9|          null|              null|              null|
|wade|  10|  4|1.4969767098E9|1.4969766781E9|126.80000019073486|317.00000047683716|
|wade|  10|  4|1.4969767383E9|1.4969767098E9|             114.0|             285.0|
|wade|  10|  4|1.4969767688E9|1.4969767383E9|             122.0|             305.0|
|wade|  10|  4|1.4969767981E9|1.4969767688E9|117.19999980926514|292.99999952316284|
|wade|  10|  4|1.4969768288E9|1.4969767981E9|122.80000019073486|307.00000047683716|
|wade|  10|  4|1.4969768585E9|1.4969768288E9|118.80000019073486|297.00000047683716|
|wade|  10|  4|1.4969768888E9|1.4969768585E9|121.19999980926514|302.99999952316284|
+----+----+---+--------------+--------------+------------------+------------------+


val aggregateUsage = interpolatedUsage.groupBy("name")
                                      .agg( sum($"GBsecs").as("TotalGBsecs"),
                                            sum($"Vcoresecs").as("TotalVcoresecs")
                                       )
aggregateUsage.show()

aggregateUsage: org.apache.spark.sql.DataFrame = [name: string, TotalGBsecs: double ... 1 more field]
+----+-----------------+-----------------+
|name|      TotalGBsecs|   TotalVcoresecs|
+----+-----------------+-----------------+
|john|          10810.0|           1081.0|
|wade|842.8000001907349|2107.000000476837|
+----+-----------------+-----------------+                                    

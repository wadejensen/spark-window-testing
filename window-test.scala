ampledUsage = Seq(
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

// Import the window functions.
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

val wspec = Window.partitionBy("name").orderBy($"timestamp".asc)
val  interpolatedUsage = df.withColumn("timestampPrev", lag( $"timestamp", 1 ).over(wspec))
                           .withColumn("GBsecs", ($"timestamp" - $"timestampPrev") * $"gb" )
                           .withColumn("VCoresecs", ($"timestamp" - $"timestampPrev") * $"cpus" )

val aggregateUsage = interpolatedUsage.groupBy("name")
                                      .agg( sum($"GBsecs").as("TotalGBsecs"),
                                            sum($"Vcoresecs").as("TotalVcoresecs")
                                       )


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

object Q1 {
    def main(args: Array[String]) {

		/*
		Job submission running command:
		$SPARK_HOME/bin/spark-submit <JAR Path> --master <spark://MASTERNODE:PORT>  --class <class-name> <input-file-path> <state-name>
		Example:
		$SPARK_HOME/bin/spark-submit target/scala-2.12/cs455_term_project_2.12-1.0.jar --master spark://madison:41278  --class Q1 /test_spark/co_combined_norm.csv CO
		
		*/

        val spark = SparkSession.builder().appName("Q1").getOrCreate()
		
        
        import spark.implicits._

		val inFile = args(4)
		val stateName = args(5)
		val inFile2 = args(6)

		//val df = spark.read.option("header",true).csv("/test_spark/combined_with_covid.csv")
		val df = spark.read.option("header",true).csv(inFile)


		// correlation between  socio Eco factor and total crime cases
		val column_name_one = "Socio_Eco_RPL1"
		val column_name_two = "TOTAL_CASES"
		val df2   = df.withColumn(column_name_one,col(column_name_one).cast(DoubleType)).withColumn(column_name_two,col(column_name_two).cast(DoubleType))
		val output = df2.stat.corr(column_name_one, column_name_two)
		val wr_output = spark.sparkContext.parallelize(Seq(("Socio_Eco_RPL1-TOTAL_CASES_correlation",output)))
        	wr_output.coalesce(1).saveAsTextFile("/user/rakibul/"+stateName+"/Socio_Eco_RPL1_RPL2-TOTAL_CASES_correlation")



       // correlation between  house composition disbaility factor and total crime cases
		val df3   = df.withColumn("HouseCompDis_RPL2",col("HouseCompDis_RPL2").cast(DoubleType)).withColumn("TOTAL_CASES",col("TOTAL_CASES").cast(DoubleType))
		val output1 = df3.stat.corr("HouseCompDis_RPL2", "TOTAL_CASES")
		val wr_output1 = spark.sparkContext.parallelize(Seq(("HouseCompDis_RPL2-TOTAL_CASES_correlation",output1)))
        	wr_output1.coalesce(1).saveAsTextFile("/user/rakibul/"+stateName+"/HouseCompDis_RPL2-TOTAL_CASES_correlation")


		// correlation between  minority status factor and total crime cases
		val df4   = df.withColumn("MinorStat_RPL3",col("MinorStat_RPL3").cast(DoubleType)).withColumn("TOTAL_CASES",col("TOTAL_CASES").cast(DoubleType))
		val output2 = df4.stat.corr("MinorStat_RPL3", "TOTAL_CASES")
		val wr_output2 = spark.sparkContext.parallelize(Seq(("MinorStat_RPL3-TOTAL_CASES_correlation",output2)))
        	wr_output2.coalesce(1).saveAsTextFile("/user/rakibul/"+stateName+"/MinorStat_RPL3-TOTAL_CASES_correlation")

       // correlation between  house transporation factor and total crime cases
		val df5   = df.withColumn("HouseTraonsport_RPL3",col("HouseTraonsport_RPL3").cast(DoubleType)).withColumn("TOTAL_CASES",col("TOTAL_CASES").cast(DoubleType))
		val output3 = df5.stat.corr("HouseTraonsport_RPL3", "TOTAL_CASES")
		val wr_output3 = spark.sparkContext.parallelize(Seq(("HouseTransport_RPL4-TOTAL_CASES_correlation",output3)))
        	wr_output3.coalesce(1).saveAsTextFile("/user/rakibul/"+stateName+"/HouseTransport_RPL3-TOTAL_CASES_correlation")


		// correlation between  SVI factor and total covid cases for 2020
		val df6   = df.withColumn("RPL_THEMES",col("RPL_THEMES").cast(DoubleType)).withColumn("cases2020",col("cases2020").cast(DoubleType))
		val output4 = df6.stat.corr("RPL_THEMES", "cases2020")
		val wr_output4 = spark.sparkContext.parallelize(Seq(("RPL_THEMES-cases2020_correlation",output4)))
        	wr_output4.coalesce(1).saveAsTextFile("/user/rakibul/"+stateName+"/RPL_THEMES-cases2020_correlation")

		// correlation between  SVI factor and total covid death cases for 2020

		val df7   = df.withColumn("RPL_THEMES",col("RPL_THEMES").cast(DoubleType)).withColumn("deaths2020",col("deaths2020").cast(DoubleType))
		val output5 = df7.stat.corr("RPL_THEMES", "deaths2020")
		val wr_output5 = spark.sparkContext.parallelize(Seq(("RPL_THEMES-deaths2020_correlation",output5)))
        	wr_output5.coalesce(1).saveAsTextFile("/user/rakibul/"+stateName+"/RPL_THEMES-deaths2020_correlation")

		// correlation between  SVI factor and total covid cases for 2021
		val df8   = df.withColumn("RPL_THEMES",col("RPL_THEMES").cast(DoubleType)).withColumn("cases2021",col("cases2021").cast(DoubleType))
		val output6 = df8.stat.corr("RPL_THEMES", "cases2021")
		val wr_output6 = spark.sparkContext.parallelize(Seq(("RPL_THEMES-cases2021_correlation",output6)))
        	wr_output6.coalesce(1).saveAsTextFile("/user/rakibul/"+stateName+"/RPL_THEMES-cases2021_correlation")

		// correlation between  SVI factor and total covid death cases for 2021
		val df9   = df.withColumn("RPL_THEMES",col("RPL_THEMES").cast(DoubleType)).withColumn("deaths2021",col("deaths2021").cast(DoubleType))
		val output7 = df9.stat.corr("RPL_THEMES", "deaths2021")
		val wr_output7 = spark.sparkContext.parallelize(Seq(("RPL_THEMES-deaths2021_correlation",output7)))
        	wr_output7.coalesce(1).saveAsTextFile("/user/rakibul/"+stateName+"/RPL_THEMES-deaths2021_correlation")


			// correlation between  Total Crime  and total covid cases for 2020
		val df10   = df.withColumn("TOTAL_CASES",col("TOTAL_CASES").cast(DoubleType)).withColumn("cases2020",col("cases2020").cast(DoubleType))
		val output8 = df10.stat.corr("TOTAL_CASES", "cases2020")
		val wr_output8 = spark.sparkContext.parallelize(Seq(("TOTAL_CASES-cases2020_correlation",output8)))
        	wr_output8.coalesce(1).saveAsTextFile("/user/rakibul/"+stateName+"/TOTAL_CASES-cases2020_correlation")

		// correlation between  TOTAL_CRIME_CASES and total covid death cases for 2020

		val df11   = df.withColumn("TOTAL_CASES",col("TOTAL_CASES").cast(DoubleType)).withColumn("deaths2020",col("deaths2020").cast(DoubleType))
		val output9 = df11.stat.corr("TOTAL_CASES", "deaths2020")
		val wr_output9 = spark.sparkContext.parallelize(Seq(("TOTAL_CASES-deaths2020_correlation",output9)))
        	wr_output9.coalesce(1).saveAsTextFile("/user/rakibul/"+stateName+"/TOTAL_CASES-deaths2020_correlation")

		// correlation between  TOTAL_CRIME_CASES and total covid cases for 2021
		val df12   = df.withColumn("TOTAL_CASES",col("TOTAL_CASES").cast(DoubleType)).withColumn("cases2021",col("cases2021").cast(DoubleType))
		val output10 = df12.stat.corr("TOTAL_CASES", "cases2021")
		val wr_output10 = spark.sparkContext.parallelize(Seq(("TOTAL_CASES-cases2021_correlation",output10)))
        	wr_output10.coalesce(1).saveAsTextFile("/user/rakibul/"+stateName+"/TOTAL_CASES-cases2021_correlation")

		// correlation between TOTAL_CRIME_CASES and total covid cases for 2020
		val df13   = df.withColumn("TOTAL_CASES",col("TOTAL_CASES").cast(DoubleType)).withColumn("deaths2021",col("deaths2021").cast(DoubleType))
		val output11 = df13.stat.corr("TOTAL_CASES", "deaths2021")
		val wr_output11 = spark.sparkContext.parallelize(Seq(("TOTAL_CASES-deaths2021_correlation",output11)))
        	wr_output11.coalesce(1).saveAsTextFile("/user/rakibul/"+stateName+"/TOTAL_CASES-deaths2021_correlation")


		
 	   	val sc = spark.sparkContext
		val combined_with_covid = sc.textFile(inFile2)
		val combined_with_covid_head_drop = combined_with_covid.mapPartitionsWithIndex {(idx, iter) => if (idx == 0) iter.drop(1) else iter }
		
		// maximum normal case for 2020
		val death_case = combined_with_covid_head_drop.map(line => (line.split(",")(1),line.split(",")(16).toInt))
		val maxKey = death_case.takeOrdered(1)(Ordering[Int].reverse.on(_._2))
		val max_rdd_death = sc.parallelize(maxKey)
		max_rdd_death.saveAsTextFile("/user/rakibul/"+stateName+"/max_covid_case_2020")
		val minKey = death_case.takeOrdered(1)(Ordering[Int].on(_._2))
		val min_rdd_death = sc.parallelize(minKey)
		min_rdd_death.coalesce(1).saveAsTextFile("/user/rakibul/"+stateName+"/min_covid_case_2020")

		// maximum death case for 2020 
 	   	
		
		val death_case1 = combined_with_covid_head_drop.map(line => (line.split(",")(1),line.split(",")(17).toInt))
		val maxKey1 = death_case1.takeOrdered(1)(Ordering[Int].reverse.on(_._2))
		val max_rdd_death1 = sc.parallelize(maxKey1)
		max_rdd_death1.saveAsTextFile("/user/rakibul/"+stateName+"/max_death_case_2020")
		val minKey1 = death_case1.takeOrdered(1)(Ordering[Int].on(_._2))
		val min_rdd_death1 = sc.parallelize(minKey1)
		min_rdd_death1.coalesce(1).saveAsTextFile("/user/rakibul/"+stateName+"/min_death_case_2020")

		// maximum normal case for 2021
 	   
		val death_case2 = combined_with_covid_head_drop.map(line => (line.split(",")(1),line.split(",")(18).toInt))
		val maxKey2 = death_case2.takeOrdered(1)(Ordering[Int].reverse.on(_._2))
		val max_rdd_death2 = sc.parallelize(maxKey2)
		max_rdd_death2.saveAsTextFile("/user/rakibul/"+stateName+"/max_covid_case_2021")
		val minKey2 = death_case2.takeOrdered(1)(Ordering[Int].on(_._2))
		val min_rdd_death2 = sc.parallelize(minKey2)
		min_rdd_death2.coalesce(1).saveAsTextFile("/user/rakibul/"+stateName+"/min_covid_case_2021")

		// maximum death case for 2021 
 	   	
		val death_case3 = combined_with_covid_head_drop.map(line => (line.split(",")(1),line.split(",")(19).toInt))
		val maxKey3 = death_case3.takeOrdered(1)(Ordering[Int].reverse.on(_._2))
		val max_rdd_death3 = sc.parallelize(maxKey3)
		max_rdd_death3.saveAsTextFile("/user/rakibul/"+stateName+"/max_death_case_2021")
		val minKey3 = death_case3.takeOrdered(1)(Ordering[Int].on(_._2))
		val min_rdd_death3 = sc.parallelize(minKey3)
		min_rdd_death3.coalesce(1).saveAsTextFile("/user/rakibul/"+stateName+"/min_death_case_2021")
            

		
	
		
       
    }
}

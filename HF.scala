package GrayMatterAnalytics

/**
  * Created by hadoop on 4/5/17.
  */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext

object HF {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("HF").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("HF").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val df = spark.read.format("com.databricks.spark.csv").option("header","true").option("inferschema","true").load("file:///home/hadoop/Downloads/Sample Data 2016.csv")
    df.printSchema()
    df.createOrReplaceTempView("analysis")

    val totcnt = spark.sql("select count(*) from analysis").show()

    def decode(s: String):Int = { Map(("Yes"->1),("No"->0))(s)}
    spark.udf.register("decode",decode(_:String))

    def stay(stays: Int): Int = {
      var stay = stays
      if (stay < 1) { stay = 0 }
      if (stay >= 4 && stay <= 6) {stay = 4}
      if (stay >= 7 && stay <= 13) {stay = 5}
      if (stay >= 14) {stay = 7}
      val code = Map(0 -> 0, 1 -> 1, 2 -> 2, 3 -> 3, 4 -> 4, 5 -> 5, 7 -> 7)
      code(stay)
    }
    spark.udf.register("stay", stay(_: Int))

    def EDvisit(EDvisits: Int): Int = {
      var EDvisit = EDvisits
      if (EDvisit < 1) { EDvisit = 0 }
      if (EDvisit >= 4) { EDvisit = 4 }
      val code = Map(0 -> 0, 1 -> 1, 2 -> 2, 3 -> 3, 4 -> 4)
      code(EDvisit)
    }
    spark.udf.register("EDvisit", EDvisit(_: Int))

    val HFDF = spark.sql("select ((EDvisit(ED_visits)+stay(LengthofStay)) + decode(HistoryofCABG)+decode(`Septicemia/shock`)+decode( Congestiveheartfailure)+decode( Acutecoronarysyndrome)+decode( Coronaryatherosclerosisoranginacerebrovasculardisease)+decode( Valvularorrheumaticheartdisease)+decode( Specifiedarrhythmias)+decode( Vascularorcirculatorydisease)+decode( OtherandUnspecifiedHeartDisease)+decode( Metastaticcanceroracuteleukemia)+decode( Cancer)+decode( `Diabetesmellitus(DM)orDMcomplications`)+decode( `Protein-caloriemalnutrition`)+decode( `Disordersoffluidelectrolyteacid-base`)+decode( Liverandbiliarydisease)+decode( PepticUlcerHemorrhageOtherSpecifiedGastrointestinalDisorders)+decode( OtherGastrointestinalDisorders)+decode( SevereHematologicalDisorders)+decode( Irondeficiencyorotheranemiasandblooddisease)+decode( Dementiaorotherspecifiedbraindisorders)+decode( `Drug/AlcoholInducedDependence/Psychosis`)+decode( MajorPsychiatricDisorders)+decode( Depression)+decode( OtherPsychiatricDisorders)+decode( Hemiplegiaparaplegiaparalysisfunctionaldisability)+decode( Stroke)+decode( Chronicobstructivepulmonarydisease)+decode( Fibrosisoflungandotherchroniclungdisorders)+decode( Asthma)+decode( Pneumonia)+decode( `End-stagerenaldiseaseordialysis`)+decode( Renalfailure)+decode( Nephritis)+decode( Otherurinarytractdisorders)+decode( Decubitusulcerorchronicskinulcer)) lacescore from analysis where diagnosis_code in (402.01,402.11,402.91,404.01,404.03,404.11,404.13,404.91,404.93,'428.xx')")
    HFDF.createOrReplaceTempView("result")
    spark.sql("set totcnt=1895")
    val resultset = spark.sql("select (lacescore/${totcnt}) Amitotscore from result where lacescore > 9").show()
    spark.stop()
  }
}

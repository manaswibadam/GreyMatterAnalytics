package GrayMatterAnalytics

/**
  * Created by hadoop on 4/5/17.
  */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext

object PN {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("PN").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("PN").getOrCreate()
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

    val PNDF  = spark.sql("select ((EDvisit(ED_visits)+stay(LengthofStay)) + decode(HistoryofCABG)+decode( Historyofinfection)+decode( `Septicemia/shock`)+decode( Metastaticcanceroracuteleukemia)+decode( LungUpperDigestiveTractandOtherSevereCancers)+decode(`Diabetesmellitus(DM)orDMcomplications`)+decode( `Protein-caloriemalnutrition`)+decode( `Disordersoffluidelectrolyteacid-base`)+decode( OtherGastrointestinalDisorders)+decode( SevereHematologicalDisorders)+decode( Irondeficiencyorotheranemiasandblooddisease)+decode( Dementiaorotherspecifiedbraindisorders)+decode( `Drug/AlcoholInducedDependence/Psychosis`)+decode( MajorPsychiatricDisorders)+decode( OtherPsychiatricDisorders)+decode( Hemiplegiaparaplegiaparalysisfunctionaldisability)+decode(`Cardio-respiratoryfailureorcardio-respiratoryshock`)+decode( Congestiveheartfailure)+decode( Acutecoronarysyndrome)+decode( Coronaryatherosclerosisoranginacerebrovasculardisease)+decode( Valvularorrheumaticheartdisease)+decode( Specifiedarrhythmias)+decode( Stroke)+decode( Vascularorcirculatorydisease)+decode( Chronicobstructivepulmonarydisease)+decode( Fibrosisoflungandotherchroniclungdisorders)+decode( Asthma)+decode( Pneumonia)+decode( `Pleuraleffusion/pneumothorax`)+decode( `End-stagerenaldiseaseordialysis`)+decode( Renalfailure)+decode( Urinarytractinfection)+decode( Otherurinarytractdisorders)+decode( Decubitusulcerorchronicskinulcer)+decode( VertebralFractures)+decode( Otherinjuries)+decode(`LymphaticHeadandNeckBrainandOtherMajorCancersBreastColorectalandotherCancersandTumorsOtherRespiratoryandHeartNeoplasms`)) lacescore from analysis where diagnosis_code Between 480.0 and 488.11")
    PNDF.createOrReplaceTempView("result")
    spark.sql("set totcnt=1895")
    val resultset = spark.sql("select (lacescore/${totcnt}) Amitotscore from result where lacescore > 9").show()
    spark.stop()
  }
}

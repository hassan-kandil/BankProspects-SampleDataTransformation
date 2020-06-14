from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType
from pyspark.sql.functions import mean

spark = SparkSession \
          .builder \
          .master("local[*]") \
          .enableHiveSupport() \
          .getOrCreate() \


"""## Extracting Bank Prospects Data Frame from CSV File"""

bankProspectsDf = spark.read.csv('data/bank_prospects.csv', header=True, inferSchema=True)
bankProspectsDf.show()

bankProspectsDf.printSchema()

"""## Changing Salary Column Datatype to Float"""

bankProspectsDf2 = bankProspectsDf.withColumn("Salary", bankProspectsDf.Salary.cast(FloatType()))
bankProspectsDf2.printSchema()

"""## Cleaning Rows with Unknown Countries"""

no_unknown_countryDf = bankProspectsDf2.filter(bankProspectsDf2.Country != 'unknown')
no_unknown_countryDf.show()

"""## Calculating Avg Age and Avg Salary"""

meanAge = no_unknown_countryDf.select(mean(no_unknown_countryDf.Age)).collect()
meanAgeVal = meanAge[0][0]
print('Mean Age Value', meanAgeVal)

meanSalary = no_unknown_countryDf.select(mean(no_unknown_countryDf.Salary)).collect()
meanSalaryVal = meanSalary[0][0]
print('Mean Salary Value', meanSalaryVal)

"""## Replacing Null Values with Avg Values"""

no_nullDf = no_unknown_countryDf.fillna({"Age": meanAgeVal, "Salary": meanSalaryVal});
no_nullDf.show()

"""## Writing the transformed data frame to a new csv file"""

no_nullDf.write.format('csv').save('BankProspects_Transformed')


"""## ## Loading Transformed Data into Hive Table """

no_nullDf.write.mode("overwrite").saveAsTable("bank.prospectclean")
spark.sql("select * from bank.prospectclean").show()

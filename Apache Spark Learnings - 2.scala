// Databricks notebook source
// MAGIC %md
// MAGIC #Spark SQL - External Data Sources

// COMMAND ----------

// DBTITLE 1,Scala UDF
val cubed = (s: Long) => {
 s * s * s
}
// Register UDF
spark.udf.register("cubed", cubed)
// Create temporary view
spark.range(1, 9).createOrReplaceTempView("udf_test")

// COMMAND ----------

// MAGIC %sql
// MAGIC select id,cubed(id) as cube_id from udf_test

// COMMAND ----------

// DBTITLE 1,Pandas UDF
// MAGIC %py
// MAGIC import pandas as pd
// MAGIC from pyspark.sql.functions import col,pandas_udf
// MAGIC from pyspark.sql.types import LongType
// MAGIC def cubed(a):
// MAGIC     return a*a*a
// MAGIC cubed_udf = pandas_udf(cubed,returnType=LongType())
// MAGIC
// MAGIC x = pd.Series([1, 2, 3])
// MAGIC print(cubed(x))

// COMMAND ----------

// MAGIC %py
// MAGIC df = spark.range(1,4)
// MAGIC df.withColumn("cubed",cubed_udf(col("id"))).show()

// COMMAND ----------

// MAGIC %md
// MAGIC #Built-in Functions

// COMMAND ----------

// MAGIC %md
// MAGIC ## Array Types

// COMMAND ----------

// DBTITLE 1,array_distinct()
// MAGIC %sql
// MAGIC SELECT array_distinct(array(1, 2, 3, null, 3));

// COMMAND ----------

// DBTITLE 1,array_intersect()
// MAGIC %sql
// MAGIC SELECT array_intersect(array(1, 2, 3), array(1, 3, 5));

// COMMAND ----------

// DBTITLE 1,array_union()
// MAGIC %sql
// MAGIC SELECT array_union(array(1, 2, 3), array(1, 3, 5));

// COMMAND ----------

// DBTITLE 1,array_except()
// MAGIC %sql
// MAGIC SELECT array_except(array(1, 2, 3), array(1, 3, 5));

// COMMAND ----------

// DBTITLE 1,array_join()
// MAGIC %sql
// MAGIC SELECT array_join(array('hello', 'world'), ' ');
// MAGIC

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT array_join(array('hello', null ,'world'), ' ');

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT array_join(array('hello', null ,'world'), ' ', ',');

// COMMAND ----------

// DBTITLE 1,array_max()
// MAGIC %sql
// MAGIC SELECT array_max(array(1, 20, null, 3));
// MAGIC

// COMMAND ----------

// DBTITLE 1,array_min()
// MAGIC %sql
// MAGIC SELECT array_min(array(1, 20, null, 3));

// COMMAND ----------

// DBTITLE 1,array_position()
// MAGIC %sql
// MAGIC SELECT array_position(array(3, 2, 1), 1);

// COMMAND ----------

// DBTITLE 1,array_remove()
// MAGIC %sql
// MAGIC SELECT array_remove(array(1, 2, 3, null, 3), 3);

// COMMAND ----------

// DBTITLE 1,arrays_overlap()
// MAGIC %sql
// MAGIC SELECT arrays_overlap(array(1, 2, 3), array(3, 4, 5));

// COMMAND ----------

// DBTITLE 1,array_sort()
// MAGIC %sql
// MAGIC SELECT array_sort(array('b', 'd', null, 'c', 'a'));

// COMMAND ----------

// DBTITLE 1,array_repeat()
// MAGIC %sql
// MAGIC SELECT array_repeat('123', 2);

// COMMAND ----------

// DBTITLE 1,arrays_zip()
// MAGIC %sql
// MAGIC SELECT arrays_zip(array(1, 2, 3), array(2, 3, 4));

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT arrays_zip(array(1, 2), array(2, 3), array(3, 4));

// COMMAND ----------

// MAGIC %md
// MAGIC ##concat()

// COMMAND ----------

// DBTITLE 1,concat()
// MAGIC %sql
// MAGIC SELECT concat('Spark', 'SQL');

// COMMAND ----------

// DBTITLE 1,concat_ws()
// MAGIC %sql
// MAGIC SELECT concat_ws(' ', 'Spark', 'SQL');

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT concat(array(1, 2, 3), array(4, 5), array(6));

// COMMAND ----------

// MAGIC %md
// MAGIC ##flatten()

// COMMAND ----------

// MAGIC %sql
// MAGIC --Flatten - ArrayOfArrays
// MAGIC SELECT flatten(array(array(1, 2), array(3, 4)));

// COMMAND ----------

// MAGIC %md
// MAGIC ##reverse()

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT reverse('Spark SQL');
// MAGIC

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT reverse(array(2, 1, 4, 3));

// COMMAND ----------

// MAGIC %md
// MAGIC ##sequence()

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT sequence(1, 5);

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT sequence(5, 1);

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT sequence(to_date('2018-01-01'), to_date('2018-03-01'), interval 1 month);

// COMMAND ----------

// MAGIC %md
// MAGIC ##shuffle()

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT shuffle(array(1, 20, 3, 5));

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT shuffle(array(1, 20, null, 3));

// COMMAND ----------

// MAGIC %md
// MAGIC ##slice()

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT slice(array(1, 2, 3, 4), 2, 3);

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT slice(array(1, 2, 3, 4), -4, 4);
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC ##Map Types

// COMMAND ----------

// DBTITLE 1,map_from_arrays()
// MAGIC %sql
// MAGIC SELECT map_from_arrays(array(1.0, 3.0), array('2', '4'));

// COMMAND ----------

// DBTITLE 1,map_from_entries()
// MAGIC %sql
// MAGIC SELECT map_from_entries(array(struct(1, 'a'), struct(2, 'b')));

// COMMAND ----------

// DBTITLE 1,map_concat()
// MAGIC %sql
// MAGIC SELECT map_concat(map(1, 'a', 2, 'b'), map(3, 'c', 4, 'd'));
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC ##Both Map and Array Types

// COMMAND ----------

// DBTITLE 1,element_at() - array()
// MAGIC %sql
// MAGIC SELECT element_at(array(10, 20, 30), 2);

// COMMAND ----------

// DBTITLE 1,element_at() - map()
// MAGIC %sql
// MAGIC SELECT element_at(map(1, 'a', 2, 'b'), 2);

// COMMAND ----------

// DBTITLE 1,cardinality() - array()
// MAGIC %sql
// MAGIC SELECT cardinality(array('b', 'd', 'c', 'a'));

// COMMAND ----------

// DBTITLE 1,cardinality() - map()
// MAGIC %sql
// MAGIC SELECT cardinality(map(1, 'a', 2, 'b'));

// COMMAND ----------

// MAGIC %md
// MAGIC #Higher Order Functions

// COMMAND ----------

// MAGIC %md
// MAGIC ##Transform

// COMMAND ----------

// DBTITLE 1,tranform()
// MAGIC %sql
// MAGIC SELECT transform(array(1, 2, 3), x -> x + 1);

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT transform(array(1, 2, 3), (x, i) -> x + i);

// COMMAND ----------

val t1 = Array(35, 36, 32, 30, 40, 42, 38)
val t2 = Array(31, 32, 34, 55, 56)
val tC = Seq(t1, t2).toDF("celsius")
tC.createOrReplaceTempView("tC")
// Show the DataFrame
tC.show()

// COMMAND ----------

// MAGIC %sql
// MAGIC select celsius,transform(celsius, x -> (x*9 div 5)+32) as fahrenheit from tC

// COMMAND ----------

val json_df = spark.read.option("multiLine",true).json("/FileStore/tables/Spark_JSON_practice.json")
json_df.createOrReplaceTempView("json_df")
display(json_df)

// COMMAND ----------

// MAGIC %sql
// MAGIC select flatten(projects.tasks) from json_df

// COMMAND ----------

// DBTITLE 1,transform() - flatten()
// MAGIC %sql
// MAGIC select transform(flatten(projects.tasks), x -> replace(x,"Task","Job")) from json_df

// COMMAND ----------

// MAGIC %md
// MAGIC ##Filter

// COMMAND ----------

// DBTITLE 1,filter()
// MAGIC %sql
// MAGIC SELECT filter(array(1, 2, 3, 4, 5), x -> x % 2 == 1);

// COMMAND ----------

// MAGIC %sql
// MAGIC select *, filter(scores,x -> x>90) as scores_greaterthan_90 from json_df

// COMMAND ----------

// DBTITLE 1,filter() - flatten()
// MAGIC %sql
// MAGIC select *, filter(flatten(projects.tasks), x->x=="Task 3") from json_df

// COMMAND ----------

// MAGIC %md
// MAGIC ##Aggregate

// COMMAND ----------

// DBTITLE 1,aggregate()
// MAGIC %sql
// MAGIC SELECT aggregate(array(1, 2, 3), 0, (acc, x) -> acc + x);

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT aggregate(array(1, 2, 3), 0, (acc, x) -> acc + x, acc -> acc * 10);

// COMMAND ----------

// DBTITLE 1,aggregate() - json()
// MAGIC %sql
// MAGIC select *, aggregate(scores, monotonically_increasing_id() , (acc,val) -> acc+val, acc->acc div 3) as score_avg from json_df

// COMMAND ----------

// DBTITLE 1,aggregate() - flatten()
// MAGIC %sql
// MAGIC select *, aggregate(flatten(projects.tasks), '' , (acc,val) -> concat_ws(' :: ',acc,val), acc->substring(acc,5)) as score_avg from json_df

// COMMAND ----------

// MAGIC %md
// MAGIC ##Exists

// COMMAND ----------

// DBTITLE 1,exists()
// MAGIC %sql
// MAGIC SELECT exists(array(1, 2, 3), x -> x % 2 == 0);

// COMMAND ----------

// MAGIC %sql
// MAGIC select *,exists(scores,x-> x>90) as `score>90` from json_df

// COMMAND ----------

// DBTITLE 1,exists() - flatten() - array_contains()
// MAGIC %sql
// MAGIC SELECT *, EXISTS(flatten(projects.tasks), x -> array_contains(array('Task 3', 'Task 6'), x)) AS task_exists FROM json_df

// COMMAND ----------

// MAGIC %md
// MAGIC ##zip_with

// COMMAND ----------

// DBTITLE 1,zip_with
// MAGIC %sql
// MAGIC select zip_with(element_at(projects.tasks,1),element_at(projects.tasks,2),(x,y) -> concat_ws('::',x,y)) from json_df

// COMMAND ----------

// DBTITLE 1,zip_with() - element_at()
// MAGIC %sql
// MAGIC select zip_with(element_at(projects.tasks,1),element_at(projects.tasks,2),(x,y) -> (x,y)) from json_df

// COMMAND ----------

// MAGIC %md
// MAGIC ##Reduce

// COMMAND ----------

// DBTITLE 1,reduce()
// MAGIC %sql
// MAGIC SELECT reduce(array(1, 2, 3), 0, (acc, x) -> acc + x);

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT reduce(array(1, 2, 3), 0, (acc, x) -> acc + x, acc -> acc * 10);

// COMMAND ----------

// DBTITLE 1,reduce() - named_struct()
// MAGIC %sql
// MAGIC SELECT *,reduce(scores,
// MAGIC                 named_struct('sum', 0, 'cnt', 0),
// MAGIC                 (acc, x) -> named_struct('sum', cast((acc.sum + x) as int), 'cnt', acc.cnt + 1),
// MAGIC                 acc -> acc.sum div acc.cnt) AS avg_scores from json_df

// COMMAND ----------

// MAGIC %md
// MAGIC #Common Dataframes and Spark SQL Operations

// COMMAND ----------

// DBTITLE 1,Load Sample Data
import org.apache.spark.sql.functions._
// Set file paths
val delaysPath =
 "/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"
val airportsPath =
 "/databricks-datasets/learning-spark-v2/flights/airport-codes-na.txt"
// Obtain airports data set
val airports = spark.read
 .option("header", "true")
 .option("inferschema", "true")
 .option("delimiter", "\t")
 .csv(airportsPath)
airports.createOrReplaceTempView("airports_na")
// Obtain departure Delays data set
val delays = spark.read
 .option("header","true")
 .csv(delaysPath)
 .withColumn("delay", expr("CAST(delay as INT) as delay"))
 .withColumn("distance", expr("CAST(distance as INT) as distance"))
delays.createOrReplaceTempView("departureDelays")

val foo = delays.filter(
 expr("""origin == 'SEA' AND destination == 'SFO' AND
 date like '01010%' AND delay > 0"""))
foo.createOrReplaceTempView("foo")

// COMMAND ----------

// DBTITLE 1,Union()
val bar = delays.union(foo)
bar.createOrReplaceTempView("bar")
bar.filter(expr("""origin == 'SEA' AND destination == 'SFO'
AND date LIKE '01010%' AND delay > 0""")).show()

// COMMAND ----------

// MAGIC %sql
// MAGIC with chay as (select * from departureDelays union all select * from foo)
// MAGIC select count(*) from chay where origin == 'SEA' AND destination == 'SFO' AND date LIKE '01010%' AND delay > 0

// COMMAND ----------

// DBTITLE 1,Join()
foo.join(
 airports.as('air),
 $"air.IATA" === $"origin"
).select("City", "State", "date", "delay", "distance", "destination").show()

// COMMAND ----------

// MAGIC %sql
// MAGIC select city,state,date,delay,distance,destination from foo join airports_na on airports_na.IATA = origin

// COMMAND ----------

// MAGIC %md
// MAGIC ##Window Functions

// COMMAND ----------

import spark.implicits._
val df = Seq(
 (1001, "a", "Electronics", 4000),
 (1002, "b", "Biotechnology", 3000),
 (1003, "c", "Computer Science", 4000),
 (1004, "d", "IT", 6500),
 (1005, "e", "Biotechnology", 3000),
 (1006, "f", "IT", 5000),
 (1007, "g", "Biotechnology", 7000),
 (1008, "h", "Electronics", 4000),
 (1009, "i", "Computer Science", 6500),
 (1010, "j", "Computer Science", 7000),
 (1011, "k", "Computer Science", 8000),
 (1012, "l", "Computer Science", 10000),
 (1013, "m", "Electronics", 2000),
 (1014, "n", "Biotechnology", 3000),
 (1015, "o", "Computer Science", 4500),
 (1016, "p", "IT", 6500),
 (1017, "q", "Biotechnology", 3500),
 (1018, "r", "IT", 5500),
 (1019, "s", "Biotechnology", 8000),
 (1020, "t", "Electronics", 5000),
 (1021, "u", "Computer Science", 6000),
 (1022, "v", "Computer Science", 8000),
 (1023, "w", "Computer Science", 8000),
 (1024, "x", "Computer Science", 11000)
).toDF("id", "name", "dept", "salary")
df.createOrReplaceTempView("employee")

// COMMAND ----------

// MAGIC %md
// MAGIC ###Ranking Functions

// COMMAND ----------

// DBTITLE 1,row_number()
// MAGIC %sql
// MAGIC select *, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY SALARY) AS row_number from employee

// COMMAND ----------

// DBTITLE 1,rank()
// MAGIC %sql
// MAGIC select *, rank() OVER (PARTITION BY dept ORDER BY SALARY) AS rank from employee

// COMMAND ----------

// DBTITLE 1,dense_rank()
// MAGIC %sql
// MAGIC select *, dense_rank() OVER (PARTITION BY dept ORDER BY SALARY) AS dense_rank from employee

// COMMAND ----------

// DBTITLE 1,percent_rank()
// MAGIC %sql
// MAGIC select *, percent_rank() OVER (PARTITION BY dept ORDER BY SALARY) AS percent_rank from employee

// COMMAND ----------

// DBTITLE 1,ntile()
// MAGIC %sql
// MAGIC select *, ntile(3) OVER (PARTITION BY dept ORDER BY SALARY) AS ntile from employee

// COMMAND ----------

// MAGIC %md
// MAGIC ###Analytic Functions

// COMMAND ----------

// DBTITLE 1,cume_dist()
// MAGIC %sql
// MAGIC select *, cume_dist() OVER (PARTITION BY dept ORDER BY SALARY) AS cume_dist from employee

// COMMAND ----------

// DBTITLE 1,lead()
// MAGIC %sql
// MAGIC select *, LEAD(salary,1,'c') OVER (PARTITION BY dept ORDER BY id) AS lead from employee

// COMMAND ----------

// DBTITLE 1,lag()
// MAGIC %sql
// MAGIC select *, LAG(salary,1,'c') OVER (PARTITION BY dept ORDER BY id) AS lag from employee

// COMMAND ----------

// DBTITLE 1,nth_value()
// MAGIC %sql
// MAGIC select *, nth_value(salary,3) OVER (PARTITION BY dept ORDER BY id) AS nth_value from employee

// COMMAND ----------

// DBTITLE 1,first_value()
// MAGIC %sql
// MAGIC select *, first_value(salary) OVER (PARTITION BY dept ORDER BY id) AS first_value from employee

// COMMAND ----------

// DBTITLE 1,last_value()*
// MAGIC %sql
// MAGIC select *, last_value(salary) OVER (PARTITION BY dept ORDER BY salary ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_value from employee

// COMMAND ----------

// MAGIC %md
// MAGIC ###Aggregate Functions

// COMMAND ----------

// DBTITLE 1,min()
// MAGIC %sql
// MAGIC select *, min(salary) OVER (PARTITION BY dept ORDER BY salary) AS min_value from employee

// COMMAND ----------

// DBTITLE 1,max()
// MAGIC %sql
// MAGIC select *, max(salary) OVER (PARTITION BY dept) AS max_value from employee

// COMMAND ----------

// DBTITLE 1,avg()
// MAGIC %sql
// MAGIC select *, avg(salary) OVER (PARTITION BY dept) AS avg_value from employee

// COMMAND ----------

// DBTITLE 1,sum()
// MAGIC %sql
// MAGIC select *, sum(salary) OVER (PARTITION BY dept) AS sum_value from employee

// COMMAND ----------

// DBTITLE 1,count()
// MAGIC %sql
// MAGIC select *, count(salary) OVER (PARTITION BY dept) AS ciunt_value from employee

// COMMAND ----------

// MAGIC %md
// MAGIC #Spark SQL AND Datasets

// COMMAND ----------

// DBTITLE 1,case class - as[<caseclassname>]
case class schema(emp_name:String,emp_salary:Int)
val rdd = Seq(("Chay",100),("Ramesh",80),("Suresh",60))
val ds = sc. parallelize(rdd).toDF("emp_name","emp_salary").as[schema]
display(ds)

// COMMAND ----------

// DBTITLE 1,case class - yield(case class())
import scala.util.Random._

case class Usage(uid:Int, uname:String, usage: Int)
val r = new scala.util.Random(42)

val data = for (i <- 1 to 20)
 yield (Usage(i, "user-" + r.alphanumeric.take(5).mkString(""), r.nextInt(1000)))

val dsUsage = spark.createDataset(data)
display(dsUsage)

// COMMAND ----------

// DBTITLE 1,dataset- filter()
dsUsage.filter(d => (d.usage>700 & d.uid<10)).show()

// COMMAND ----------

// DBTITLE 1,dataset - filter() - another class
def filterWithUsage(u: Usage) = u.usage > 700 & u.uid>10
dsUsage.filter(filterWithUsage(_)).show()

// COMMAND ----------

def filterOnEmployee(e:schema) = e.emp_salary>=80 & e.emp_name=="Chay"
ds.filter(filterOnEmployee(_)).show()

// COMMAND ----------

// DBTITLE 1,dataset - map()
dsUsage.map(ds => (if (ds.uid<=10) ds.usage*10 else ds.usage*100)).show(5)

// COMMAND ----------

// DBTITLE 1,dataset - map() - another class || case class
case class TransformedUsage(username: String, adjustedUsage: Double)
def mapWithUsage(u:Usage):Double = (if (u.uid>10) u.usage*10 else u.usage*100)
dsUsage.map(u=>{TransformedUsage(u.uname,mapWithUsage(u))}).show(5)

// COMMAND ----------

case class UsageCost(uid: Int, uname:String, usage: Int, cost: Double)

def computeUserCostUsage(u: Usage): UsageCost = {
 val v = if (u.usage > 750) u.usage * 0.15 else u.usage * 0.50
 UsageCost(u.uid, u.uname, u.usage, v)
}

dsUsage.map(u => {computeUserCostUsage(u)}).show(5)

// COMMAND ----------

// MAGIC %md
// MAGIC #Spark Join Strategies

// COMMAND ----------

val emp = Seq((1, "Smith", -1, "2018","10", "M", 3000),
(2, "Rose", 1, "2010", "20","M", 4000), 
(3,"Williams",1,"2010","10","M",1000),
(4, "Jones",2,"2005","10", "F", 2000),
(5, "Brown",2 ,"2010", "40","",-1),
(6, "Brown",2, "2010","50","",-1))

val empColumns = Seq("emp_id", "name", "superior_emp_id","year_joined", "emp_dept_id", "gender", "salary")

val emp_df = emp.toDF(empColumns:_*)
emp_df.createOrReplaceTempView("employee")

// COMMAND ----------

val dept = Seq(("Finance",10),
("Marketing",20),
("Sales",30),
("IT",40)
)
val deptColumns = Seq("dept_name","dept_id")

val dept_df = dept.toDF(deptColumns:_*)
dept_df.createOrReplaceTempView("department")

// COMMAND ----------

emp_df.show()
dept_df.show()

// COMMAND ----------

// DBTITLE 1,Inner Join - Scala
val innerjoin_df = emp_df.join(dept_df,emp_df("emp_dept_id") === dept_df("dept_id"),"inner")
display(innerjoin_df)

// COMMAND ----------

// DBTITLE 1,Inner Join - SparkSQL
// MAGIC %sql
// MAGIC select e.*,d.* from employee e INNER JOIN department d on e.emp_dept_id = d.dept_id

// COMMAND ----------

// DBTITLE 1,Outer Join - Scala - outer, full, fullouter, full_outer
val outer_join_df = emp_df.join(dept_df,emp_df("emp_dept_id") === dept_df("dept_id"),"outer")
val full_join_df = emp_df.join(dept_df,emp_df("emp_dept_id") === dept_df("dept_id"),"full")
val fullouter_join_df = emp_df.join(dept_df,emp_df("emp_dept_id") === dept_df("dept_id"),"fullouter")
val full_outer_join_df = emp_df.join(dept_df,emp_df("emp_dept_id") === dept_df("dept_id"),"full_outer")
outer_join_df.show()
full_join_df.show()
fullouter_join_df.show()
full_outer_join_df.show()

// COMMAND ----------

// DBTITLE 1,Outer Join - SparkSQL - full join, full outer join
// MAGIC %sql
// MAGIC select e.*,d.*,"full join" as clause from employee e full join department d on e.emp_dept_id = d.dept_id
// MAGIC UNION all
// MAGIC select e.*,d.*,"full outer join" as clause from employee e full outer join department d on e.emp_dept_id = d.dept_id

// COMMAND ----------

// DBTITLE 1,Left Join - Scala - left, leftouter, left_outer
emp_df.join(dept_df,emp_df("emp_dept_id") === dept_df("dept_id"),"left").show()
emp_df.join(dept_df,emp_df("emp_dept_id") === dept_df("dept_id"),"leftouter").show()
emp_df.join(dept_df,emp_df("emp_dept_id") === dept_df("dept_id"),"left_outer").show()

// COMMAND ----------

// DBTITLE 1,Left Join - SparkSQL - left join, left outer join
// MAGIC %sql
// MAGIC select e.*,d.*,"left join" as clause from employee e left join department d on e.emp_dept_id = d.dept_id
// MAGIC UNION all
// MAGIC select e.*,d.*,"left outer join" as clause from employee e left outer join department d on e.emp_dept_id = d.dept_id

// COMMAND ----------

// DBTITLE 1,Right Join - Scala - right, rightouter, right_outer
emp_df.join(dept_df, emp_df("emp_dept_id") === dept_df("dept_id"),"right").show()
emp_df.join(dept_df, emp_df("emp_dept_id") === dept_df("dept_id"),"rightouter").show()
emp_df.join(dept_df, emp_df("emp_dept_id") === dept_df("dept_id"),"right_outer").show()

// COMMAND ----------

// DBTITLE 1,Right Join - SparkSQL - right join, right outer join
// MAGIC %sql
// MAGIC select e.*,d.*,"right join" as clause from employee e right join department d on e.emp_dept_id = d.dept_id
// MAGIC UNION all
// MAGIC select e.*,d.*,"right outer join" as clause from employee e right outer join department d on e.emp_dept_id = d.dept_id

// COMMAND ----------

// DBTITLE 1,Left Anti Join - Scala - anti, leftanti, left_anti
emp_df.join(dept_df, emp_df("emp_dept_id") === dept_df("dept_id"), "anti").show()
emp_df.join(dept_df, emp_df("emp_dept_id") === dept_df("dept_id"), "leftanti").show()
emp_df.join(dept_df, emp_df("emp_dept_id") === dept_df("dept_id"), "left_anti").show()

// COMMAND ----------

// DBTITLE 1,Left Anti Join - SparkSQL - anti join, left anti join
// MAGIC %sql
// MAGIC select *,"anti join" as clause from employee e anti join department d on e.emp_dept_id = d.dept_id
// MAGIC union all
// MAGIC select *,"left anti join" as clause from employee e left anti join department d on e.emp_dept_id = d.dept_id

// COMMAND ----------

// DBTITLE 1,Left Semi Join - Scala - semi, leftsemi, left_semi
emp_df.join(dept_df, emp_df("emp_dept_id") === dept_df("dept_id"), "semi").show()
emp_df.join(dept_df, emp_df("emp_dept_id") === dept_df("dept_id"), "leftsemi").show()
emp_df.join(dept_df, emp_df("emp_dept_id") === dept_df("dept_id"), "left_semi").show()

// COMMAND ----------

// DBTITLE 1,Left Semi Join - SparkSQL - semi join, left semi join
// MAGIC %sql
// MAGIC select *,"semi join" as clause from employee e semi join department d on e.emp_dept_id = d.dept_id
// MAGIC union all
// MAGIC select *,"left semi join" as clause from employee e left semi join department d on e.emp_dept_id = d.dept_id

// COMMAND ----------

// MAGIC %md
// MAGIC #SparkSQL Join Strategies - Advanced

// COMMAND ----------

val employee_df = spark.read.option("header", true).option("inferSchema", true).csv("/FileStore/tables/Employee.txt")
employee_df.createOrReplaceTempView("employee")

// COMMAND ----------

val department_df = spark.read.option("header", true).option("inferSchema", true).csv("/FileStore/tables/Department.txt")
department_df.createOrReplaceTempView("department")

// COMMAND ----------

// DBTITLE 1,Enable Broadcast Join
spark.conf.set("spark.sql.autoBroadcastJoinThreshold",104857600)

// COMMAND ----------

// DBTITLE 1,BroadcastNestedLoopJoin
employee_df.join(department_df).explain()

// COMMAND ----------

// DBTITLE 1,BroadcastHashJoin
employee_df.join(department_df,employee_df(" Dept ID ") === department_df("Dept No")).explain()

// COMMAND ----------

// DBTITLE 1,SortMergeJoin Configuration
spark.conf.get("spark.sql.join.preferSortMergeJoin")

// COMMAND ----------

// DBTITLE 1,Disable BroadCast Join
spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)

// COMMAND ----------

// DBTITLE 1,Cartesian Join
employee_df.join(department_df).explain()

// COMMAND ----------

// DBTITLE 1,Broadcast Join - broadcast()
import org.apache.spark.sql.functions._
employee_df.join(broadcast(department_df),employee_df(" Dept ID ") === department_df("Dept No")).explain()

// COMMAND ----------

spark.conf.get("spark.sql.autoBroadcastJoinThreshold")

// COMMAND ----------

// DBTITLE 1,SortMergeJoin
employee_df.join(department_df,employee_df(" Dept ID ") === department_df("Dept No")).explain()

// COMMAND ----------

// DBTITLE 1,Enable BroadCast Join
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10000000)

// COMMAND ----------

employee_df.join(department_df,employee_df(" Dept ID ") === department_df("Dept No")).explain()

// COMMAND ----------

// MAGIC %md
// MAGIC #Join Hints

// COMMAND ----------

// MAGIC %md
// MAGIC ##Broadcast Join

// COMMAND ----------

// DBTITLE 1,broadcast
employee_df.join(department_df.hint("broadcast"),employee_df(" Dept ID ") === department_df("Dept No")).explain()

// COMMAND ----------

// DBTITLE 1,broadcastjoin
employee_df.join(department_df.hint("broadcastjoin"),employee_df(" Dept ID ") === department_df("Dept No")).explain()

// COMMAND ----------

// DBTITLE 1,mapjoin
employee_df.join(department_df.hint("mapjoin"),employee_df(" Dept ID ") === department_df("Dept No")).explain()

// COMMAND ----------

// MAGIC %md
// MAGIC ##Sort Merge Join

// COMMAND ----------

// DBTITLE 1,shufflemerge
employee_df.hint("shuffle_merge").join(department_df,employee_df(" Dept ID ") === department_df("Dept No")).explain()

// COMMAND ----------

// DBTITLE 1,mergejoin
employee_df.hint("mergejoin").join(department_df,employee_df(" Dept ID ") === department_df("Dept No")).explain()

// COMMAND ----------

// DBTITLE 1,merge
employee_df.hint("merge").join(department_df,employee_df(" Dept ID ") === department_df("Dept No")).explain()

// COMMAND ----------

// MAGIC %md
// MAGIC ##Shuffle Hash Join

// COMMAND ----------

// DBTITLE 1,shuffle_hash
employee_df.hint("shuffle_hash").join(department_df,employee_df(" Dept ID ") === department_df("Dept No")).explain()

// COMMAND ----------

// MAGIC %md
// MAGIC ##Shuffle and Replicate nested loop join

// COMMAND ----------

// DBTITLE 1,shuffle_replicate
employee_df.hint("shuffle_replicate_nl").join(department_df,employee_df(" Dept ID ") === department_df("Dept No")).explain()

// COMMAND ----------



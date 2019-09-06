import java.sql.{Connection, DriverManager, ResultSet, Statement, PreparedStatement};
import java.io._
import org.postgresql.Driver;
import scala.io.Source;
import scala.util.control.NonFatal
import scala.collection.mutable.StringBuilder

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession

// trait dbConn {
//     val conn: Connection
// }

// class pgJDBCConnection(db_name: Option[String], user: Option[String], pass: Option[String], server_ip: Option[String], port: Option[String]) {

//     def conn: Connection;
//     val conn_str: String = "jdbc:postgresql://" + server_ip.getOrElse("") + ":" + port.getOrElse("") + "/" + db_name.getOrElse("") + "?user=" + user.getOrElse("") + "&password=" + pass.getOrElse("");
    
//     // connect to database
//     try {
//         //val conn: Connection = DriverManager.getConnection(conn_str);
//         // conn = Some(DriverManager.getConnection(conn_str));
//         conn = DriverManager.getConnection(conn_str);
//     }
//     catch {
//         case NonFatal(e) => e.printStackTrace;
//         //case e: Exception => e.printStackTrace;
//     }
    
    
//     // get connection
//     def getConn(): Connection = {
//         conn;
//     }

//     def getStatement(): Statement = {
//         conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
//         // conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
//         // conn match {
//         //     //case None => None: Option[Statement]
//         //     case Some(value) => value.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
//         // }
//     }


//     def closeConn() {
//         // conn.map(c => c.close);
//         conn.close;
//     }

// }

// for reading CSV files and loading them to postgres
class readAllData(db_name: Option[String], user: Option[String], pass: Option[String], server_ip: Option[String], port: Option[String]) {
// object readAllData() {
    val conn_str: String = "jdbc:postgresql://" + server_ip.getOrElse("") + ":" + port.getOrElse("") + "/" + db_name.getOrElse("") + "?user=" + user.getOrElse("") + "&password=" + pass.getOrElse("");
    println(conn_str)
    val sqlStr = new StringBuilder();
    val conn = DriverManager.getConnection(conn_str);
    val tableName: String = "balloonData_subdata_1";

    def readAndLoadToDB()
    {
        // connect to postgres DB
        // val dbConn: pgJDBCConnection = new pgJDBCConnection(db_name, user, pass, server_ip, port);
        // val conn: Connection = dbConn.getConn();
        // val stmnt: Statement = dbConn.getStatement();
        val stmnt: Statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

        // create main data table if not existing. 
        stmnt.execute("DROP TABLE IF EXISTS " + tableName);
        stmnt.execute("CREATE TABLE IF NOT EXISTS " + tableName + "(ID VARCHAR, SOUNDING_DATE INT, HOUR INT, RELTIME INT, NUMLEV INT, P_SRC VARCHAR, NP_SRC VARCHAR, LAT INT, LON INT, LVLTYP1 INT, LVLTYP2 INT, ETIME INT, PRESS INT, PFLAG VARCHAR, GPH INT, ZFLAG VARCHAR, TEMP INT, TFLAG VARCHAR, RH INT, DPDP INT, WDIR INT, WSPD INT)");

        // CSV data file locations to read
        //val fileName: List[String] = List("data/USM00070219-data.txt", "data/USM00070261-data.txt", "data/USM00070308-data.txt", "data/USM00070361-data.txt", "data/USM00070398-data.txt");
        val fileName: List[String] = List("subdata_1.txt");

        for (file <- fileName) {
            println("Reading file: " + file + " . . . . ");
            val lines: List[String] = Source.fromFile(file).getLines.toList;
            println("num of records (with headers): " + lines.size);

            var count: Int = 0;
            var h_rec: List[String] = null;
            var data_rec: List[String] = null;
        
            println("Inserting data for file: " + file + ". This will take some time!"); 

            // parse all lines for header rec and extract all data rows for the header record.
            // TODO: change to bulk insert!!
            lines.foreach(l => {
                // grab header record
                if (l.contains("#")) {
                    sqlStr.setLength(0)
                    sqlStr.append("insert into " + tableName + "(ID, SOUNDING_DATE, HOUR, RELTIME, NUMLEV, P_SRC, NP_SRC, LAT, LON, LVLTYP1, LVLTYP2, ETIME, PRESS, PFLAG, GPH, ZFLAG, TEMP, TFLAG, RH, DPDP, WDIR, WSPD)  values ");
                    h_rec = null;
                    count = 0;
                    h_rec = List(l.slice(0,1), l.slice(1,12), l.slice(13, 17)+l.slice(18,20)+l.slice(21,23), l.slice(24,26), l.slice(27,31), l.slice(32,36), l.slice(37,45), l.slice(46,54), l.slice(55,62), l.slice(63,71));
                } 
            
                // get all data records per above header
                if (!l.contains("#")) {
                    data_rec = null;
                    data_rec = List(l.slice(0,1), l.slice(1,2), l.slice(3,8), l.slice(9,15), l.slice(15,16), l.slice(16,21), l.slice(21,22), l.slice(22,27), l.slice(27,28), l.slice(28,33), l.slice(34,39), l.slice(40,45), l.slice(46,51));
                
                    // add all data per sounding to insert query and cast all columns to correct data type
                    val query: String = "('"+h_rec(1).strip+"',"+h_rec(2).strip.toInt+","+h_rec(3).strip.toInt+","+h_rec(4).strip.toInt+","+h_rec(5).strip.toInt+",'"+h_rec(6).strip+"','"+h_rec(7).strip+"',"+h_rec(8).strip.toInt+","+h_rec(9).strip.toInt+","+data_rec(0).strip.toInt+","+data_rec(1).strip.toInt+","+data_rec(2).strip.toInt+","+data_rec(3).strip.toInt+",'"+data_rec(4).strip+"',"+data_rec(5).strip.toInt+",'"+data_rec(6).strip+"',"+data_rec(7).strip.toInt+",'"+data_rec(8).strip+"',"+data_rec(9).strip.toInt+","+data_rec(10).strip.toInt+","+data_rec(11).strip.toInt+","+data_rec(12).strip.toInt+"),";
                    sqlStr.append(query);

                    count += 1;

                    // check if last record for the sounding is reached in the file
                    if (count == h_rec(5).strip.toInt) {
                        // remove trailing comma and white space from SQL command
                        //sqlStr = sqlStr.strip.stripSuffix(",");
                        
                        // insert all values to DB table for given sounding record 
                        // println(sqlStr.stripSuffix(",").toString)
                        stmnt.execute(sqlStr.stripSuffix(",").toString);
                    }
                }
            })
            // msg when finished loading a CSV file
            println("Finished loading: " + file)
        }
        // dbConn.closeConn();
        conn.close;
    }
}


object SparkTestMain {
    def main(args: Array[String])
    {
        val db_name: Option[String] = Some(args(0))
        val user: Option[String] = Some(args(1))
        val pass: Option[String] = Some(args(2))
        val server_ip: Option[String] = Some(args(3))
        val port: Option[String] = Some(args(4))
        
        val tableName: String = "balloonData_subdata_1";


        // first read all data
        /* Please un/comment to load CSVs to PostgreSQL as required. */
        // readAllData.readAndLoadToDB();
        val readData = new readAllData(db_name, user, pass, server_ip, port);
        readData.readAndLoadToDB();


        // init spark context and import implicits
        val conf = new SparkConf().setAppName("SPARKTEST1")
                                .setMaster("local[*]")
                                .set("spark.driver.memory", "8g");
        val sc = new SparkContext(conf);
        val spark = SparkSession.builder().appName("Spark reading jdbc").getOrCreate();
        import spark.implicits._;
        
        // load the postgres table in the current spark session and partition based on GPH
        val jdbcDF = spark.read.format("jdbc")
                        .option("url", "jdbc:postgresql://"+server_ip.getOrElse("")+":"+port.getOrElse("")+"/" + user.getOrElse(""))
                        .option("dbtable", tableName)
                        .option("user", user.getOrElse(""))
                        .option("password", pass.getOrElse(""))
                        .load();
        jdbcDF.createOrReplaceTempView(tableName);
        
        
        // get max GPH height
        val df = spark.sql("select max(GPH) as GMAX from " + tableName + "");
        val g_max = df.first().getInt(0);

        // write all data for GPH -9999 and -8888 codes
        val dfGPH_QA = spark.sql("select * from " + tableName + " where GPH < 0 ");
        dfGPH_QA.write.partitionBy("GPH").parquet("data/" + tableName + "_mssing_QA.parquet");


        // read data in chunks of 1000 GPH and dump as parquet files
        var height: Int = 0;
        while (height < g_max+1) {
            val height_lim: Int = height + 1000;

            val dfGPH = spark.sql("select *, floor((GPH-1)/1000) as PARTITION from " + tableName + " where GPH > " + height + " and GPH <= " + height_lim);
            dfGPH.write.partitionBy("PARTITION", "ID").parquet("data/" + tableName + height + "_" + height_lim + ".parquet");

            height += 1000;
        }
    println("END!!!!")
    }
}


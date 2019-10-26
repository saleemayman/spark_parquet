import java.sql.{Connection, DriverManager, ResultSet, Statement, PreparedStatement};
import java.io._
import org.postgresql.Driver;
import scala.io.Source;
import scala.util.control.NonFatal
import scala.collection.mutable.StringBuilder

// import org.apache.spark.implicits._;
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession


// for reading CSV files and loading them to postgres
class readAllData(table_name: Option[String], db_name: Option[String], user: Option[String], pass: Option[String], server_ip: Option[String], port: Option[String]) {
    val conn_str: String = "jdbc:postgresql://" + server_ip.getOrElse("") + ":" + port.getOrElse("") + "/" + db_name.getOrElse("") + "?user=" + user.getOrElse("") + "&password=" + pass.getOrElse("");
    val sqlStr = new StringBuilder();

    // connect to postgres DB
    val conn = DriverManager.getConnection(conn_str);

    def readAndLoadToDB()
    {
        val stmnt: Statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

        // create main data table if not existing. 
        stmnt.execute("DROP TABLE IF EXISTS " + table_name.getOrElse(""));
        stmnt.execute("CREATE TABLE IF NOT EXISTS " + table_name.getOrElse("") + "(ID VARCHAR, SOUNDING_DATE INT, HOUR INT, RELTIME INT, NUMLEV INT, P_SRC VARCHAR, NP_SRC VARCHAR, LAT INT, LON INT, LVLTYP1 INT, LVLTYP2 INT, ETIME INT, PRESS INT, PFLAG VARCHAR, GPH INT, ZFLAG VARCHAR, TEMP INT, TFLAG VARCHAR, RH INT, DPDP INT, WDIR INT, WSPD INT)");

        // CSV data file locations to read
        // val fileName: List[String] = List("data/USM00070219-data.txt");
        // val fileName: List[String] = List("data/USM00070219-data.txt", "data/USM00070261-data.txt", "data/USM00070308-data.txt", "data/USM00070361-data.txt", "data/USM00070398-data.txt");
        val fileName: List[String] = List("subdata.txt");
        // scala> import java.io.File
        // scala> new java.io.File(fileName(0)).exists
        // res12: Boolean = true
        // scala> scala.reflect.io.File(fileName(0)).exists
        // res13: Boolean = true



        for (file <- fileName) {
            println("Reading file: " + file + " . . . . ");
            // TODO: use iterator for full data
            val lines: Iterator[String] = Source.fromFile(file).getLines();
            // println("num of records (with headers): " + lines.size);

            var header_count: Int = 0;
            var data_count: Int = 0;
            // var h_recs: List[String] = null; // TODO: filter the headers before looping over the full data_recs
            // var data_rec: List[String] = null;
        
            println("Inserting data for file: " + file + ". This will take some time!"); 

            // filter header records starting with #
            val headers: Iterator[String] = lines.filter(_.startsWith("#"));
            val dataRecs: Iterator[String] = lines.filterNot(_.startsWith("#"));

            while (headers.hasNext) {
                sqlStr.setLength(0);
                sqlStr.append("insert into " + table_name.getOrElse("") + "(ID, SOUNDING_DATE, HOUR, RELTIME, NUMLEV, P_SRC, NP_SRC, LAT, LON, LVLTYP1, LVLTYP2, ETIME, PRESS, PFLAG, GPH, ZFLAG, TEMP, TFLAG, RH, DPDP, WDIR, WSPD)  values ");

                val l: String = headers.next()
                val header: List[String] = (List(l.slice(0,1), l.slice(1,12), 
                                                l.slice(13, 17)+l.slice(18,20)+l.slice(21,23), 
                                                l.slice(24,26), l.slice(27,31), l.slice(32,36), l.slice(37,45), 
                                                l.slice(46,54), l.slice(55,62), l.slice(63,71)))
                val h_rec: List[String] = header.map(a => a.replaceAll(" ", ""));

                data_count = 0;
                val currentDataRecs: Int = h_rec(5).toInt;

                while (data_count < currentDataRecs && dataRecs.hasNext) {
                    val d: String = dataRecs.next()
                    val data: List[String] = (List(d.slice(0,1), d.slice(1,2), d.slice(3,8), d.slice(9,15), d.slice(15,16), 
                                                    d.slice(16,21), d.slice(21,22), d.slice(22,27), d.slice(27,28), d.slice(28,33), 
                                                    d.slice(34,39), d.slice(40,45), d.slice(46,51)));
                    val data_rec: List[String] = data.map(a => a.replaceAll(" ", ""))

                    // add all data per sounding to insert query and cast all columns to correct data type
                    val query: String = ("('"+h_rec(1)+"',"+h_rec(2).toInt+","+h_rec(3).toInt+","+h_rec(4).toInt+","+h_rec(5).toInt+",'"+h_rec(6)+"','"+h_rec(7)+
                                        "',"+h_rec(8).toInt+","+h_rec(9).toInt+","+data_rec(0).toInt+","+data_rec(1).toInt+","+data_rec(2).toInt+","+data_rec(3).toInt+
                                        ",'"+data_rec(4)+"',"+data_rec(5).toInt+",'"+data_rec(6)+"',"+data_rec(7).toInt+",'"+data_rec(8)+"',"+data_rec(9).toInt+
                                        ","+data_rec(10).toInt+","+data_rec(11).toInt+","+data_rec(12).toInt+"),");
                    sqlStr.append(query);
                    
                    // check if last record for the sounding is reached in the file
                    data_count += 1;
                    if (data_count == currentDataRecs) {
                        // insert all values to DB table for given sounding record 
                        stmnt.execute(sqlStr.stripSuffix(",").toString);
                    }

                }

                header_count += 1;
            }

            // // parse all lines for header rec and extract all data rows for the header record.
            // // TODO: change to bulk insert!!
            // lines.foreach(l => {
            //     // grab header record
            //     if (l.contains("#")) {
            //         sqlStr.setLength(0)
            //         sqlStr.append("insert into " + table_name.getOrElse("") + "(ID, SOUNDING_DATE, HOUR, RELTIME, NUMLEV, P_SRC, NP_SRC, LAT, LON, LVLTYP1, LVLTYP2, ETIME, PRESS, PFLAG, GPH, ZFLAG, TEMP, TFLAG, RH, DPDP, WDIR, WSPD)  values ");
            //         h_rec = null;
            //         count = 0;
            //         val h_rec_tmp: List[String] = List(l.slice(0,1), l.slice(1,12), l.slice(13, 17)+l.slice(18,20)+l.slice(21,23), l.slice(24,26), l.slice(27,31), l.slice(32,36), l.slice(37,45), l.slice(46,54), l.slice(55,62), l.slice(63,71));
            //         h_rec = h_rec_tmp.map(a => a.replaceAll(" ", ""))
            //     } 
            
            //     // get all data records per above header
            //     if (!l.contains("#")) {
            //         //data_rec = null;
            //         val data_rec_tmp: List[String] = List(l.slice(0,1), l.slice(1,2), l.slice(3,8), l.slice(9,15), l.slice(15,16), l.slice(16,21), l.slice(21,22), l.slice(22,27), l.slice(27,28), l.slice(28,33), l.slice(34,39), l.slice(40,45), l.slice(46,51));
            //         val data_rec: List[String] = data_rec_tmp.map(a => a.replaceAll(" ", ""))
                
            //         // add all data per sounding to insert query and cast all columns to correct data type
            //         val query: String = "('"+h_rec(1)+"',"+h_rec(2).toInt+","+h_rec(3).toInt+","+h_rec(4).toInt+","+h_rec(5).toInt+",'"+h_rec(6)+"','"+h_rec(7)+"',"+h_rec(8).toInt+","+h_rec(9).toInt+","+data_rec(0).toInt+","+data_rec(1).toInt+","+data_rec(2).toInt+","+data_rec(3).toInt+",'"+data_rec(4)+"',"+data_rec(5).toInt+",'"+data_rec(6)+"',"+data_rec(7).toInt+",'"+data_rec(8)+"',"+data_rec(9).toInt+","+data_rec(10).toInt+","+data_rec(11).toInt+","+data_rec(12).toInt+"),";
            //         sqlStr.append(query);

            //         count += 1;

            //         // check if last record for the sounding is reached in the file
            //         if (count == h_rec(5).toInt) {
            //             // insert all values to DB table for given sounding record 
            //             stmnt.execute(sqlStr.stripSuffix(",").toString);
            //         }
            //     }
            // })
            // msg when finished loading a CSV file
            println("Finished loading: " + file)
        }
        // dbConn.closeConn();
        conn.close;
    }
}


object SparkTestMain {
    // // init spark context and import implicits
    // val conf = new SparkConf().setAppName("SPARKTEST1")
    //                         .setMaster("local[*]")
    //                         .set("spark.driver.memory", "2g");
    // val sc = new SparkContext(conf);
    // val spark = SparkSession.builder().appName("Spark reading jdbc").getOrCreate();
    // import spark.implicits._;
    

    // def readDataWithSpark() {
    //     val fileName: List[String] = List("data/USM00070219-data.txt");

    //     val lines: RDD[String] = sc.textFile(fileName, 4);
    //     val headers: RDD[String] = lines.filter(_.startsWith("#"));
    //     val records: RDD[String] = lines.filter(x => !x.startsWith("#"));

    //     // extract relevant data from header rows, convert each header rec to a list and remove whitespaces
    //     val headersList: RDD[List[String]] = (headers.map(l => List(l.slice(0,1), l.slice(1,12), l.slice(13, 17)+l.slice(18,20)+l.slice(21,23), 
    //                                                             l.slice(24,26), l.slice(27,31), l.slice(32,36), l.slice(37,45), l.slice(46,54), 
    //                                                             l.slice(55,62), l.slice(63,71)))
    //                                                     .map(x => x.map(_.trim)).zipWithIndex());
    //     // extract relevant data from records, convert each data rec to a list and remove whitespaces
    //     val recordsList: RDD[List[String]] = (records.map(d => List(d.slice(0,1), d.slice(1,2), d.slice(3,8), d.slice(9,15), d.slice(15,16), d.slice(16,21), 
    //                                                                         d.slice(21,22), d.slice(22,27), d.slice(27,28), d.slice(28,33), d.slice(34,39), 
    //                                                                         d.slice(40,45), d.slice(46,51)))
    //                                                     .map(x => x.map(_.trim)).zipWithIndex());

    //     // num of recs for the current header record
    //     val numRecs: Int = headersList.first()(5).toInt; 
    //     var currentDataRecs: Int = 0;
    //     headersList.foreach(l => {
    //         currentDataRecs += 1;

    //     })

    // }

    def main(args: Array[String])
    {
        val db_name: Option[String] = Some(args(0))
        val user: Option[String] = Some(args(1))
        val pass: Option[String] = Some(args(2))
        val server_ip: Option[String] = Some(args(3))
        val port: Option[String] = Some(args(4))
        val tableName: Option[String] = Some(args(5));
        
        println(db_name)
        println(user)
        println(pass)
        println(server_ip)
        println(tableName)
        

        // first read all data
        val readData = new readAllData(tableName, db_name, user, pass, server_ip, port);
        readData.readAndLoadToDB();


        // init spark context and import implicits
        val conf = new SparkConf().setAppName("SPARKTEST1")
                                .setMaster("local[*]")
                                .set("spark.driver.memory", "2g");
        val sc = new SparkContext(conf);
        val spark = SparkSession.builder().appName("Spark reading jdbc").getOrCreate();
        import spark.implicits._;
        
        // load the postgres table in the current spark session and partition based on GPH
        val jdbcDF = spark.read.format("jdbc")
                        .option("url", "jdbc:postgresql://"+server_ip.getOrElse("")+":"+port.getOrElse("")+"/" + user.getOrElse(""))
                        .option("dbtable", tableName.getOrElse(""))
                        .option("user", user.getOrElse(""))
                        .option("password", pass.getOrElse(""))
                        .load();
        jdbcDF.createOrReplaceTempView(tableName.getOrElse(""));
        
        
        // get max GPH height
        val df = spark.sql("select max(GPH) as GMAX from " + tableName.getOrElse("") + "");
        val g_max = df.first().getInt(0);
        println("Maximum height (m): " + g_max.toString)

        // write all data for GPH -9999 and -8888 codes
        val dfGPH_QA = spark.sql("select * from " + tableName.getOrElse("") + " where GPH < 0 ");
        dfGPH_QA.write.partitionBy("GPH").parquet("data/" + tableName.getOrElse("") + "_mssing_QA.parquet");


        // read data in chunks of 1000 GPH and dump as parquet files
        var height: Int = 0;
        while (height < g_max+1) {
            val height_lim: Int = height + 1000;

            val dfGPH = spark.sql("select *, floor((GPH-1)/1000) as PARTITION from " + tableName.getOrElse("") + " where GPH > " + height + " and GPH <= " + height_lim);
            dfGPH.write.partitionBy("PARTITION", "ID").parquet("data/" + tableName.getOrElse("") + height + "_" + height_lim + ".parquet");

            height += 1000;
        }

        sc.stop()
        println("\nEND!!!!\n")
    }
}


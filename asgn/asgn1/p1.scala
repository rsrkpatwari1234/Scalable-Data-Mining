// References followed:
// 1. https://stackoverflow.com/questions/10804581/read-case-class-object-from-string-in-scala-something-like-haskells-read-typ
// 2. https://github.com/deanwampler/spark-scala-tutorial/blob/master/src/main/scala/sparktutorial/InvertedIndex5b.scala

import java.util.TimeZone
import java.text.SimpleDateFormat


TimeZone.setDefault(TimeZone.getTimeZone("GMT")) // set default time zone.
var timeFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX") // format of time entry.
//case class LogEntry (debug_level:String, timestamp:java.util.Date, download_id:String, retrieval_stage:String, rest:String) // schema of the log entry.

case class torrentSchema (
    debug_level : String, 
    timestamp : java.util.Date, 
    download_id : String, 
    retrieval_stage : String, 
    rest : String)

def parseFile(filename: String): org.apache.spark.rdd.RDD[torrentSchema] = {
    // function to load file into RDD with LogEntry schema.
    var textRDD = sc.textFile(filename)
    var parsedRDD = textRDD
    .filter(line => line.size > 0)
    .map(line => line.split(", | -- ", 4)) // split by ", " or " -- "
    .map(w => 
    if (w.size == 4) 
        Array(w(0), w(1), w(2)) ++ w(3).split(": ",2) 
    else 
        w)
    .map(x => 
        try {
            torrentSchema(x(0), timeFormat.parse(x(1)), x(2), x(3), x(4))
        }
        catch { 
            case e: Exception => torrentSchema(null,null,null,null,null)
        }
    )
    
    return parsedRDD
}

def parseRepoName(rest: String): String={
    try{
        if (rest.contains("github.com/repos/")) {
            var words = rest.split("github.com/repos/")(1).split("\\?")
            var path = words(0).split("/")
            if (path.size == 1) {
                return path(0)
            }
            else if (path.size > 1) {
                return path(0)+"/"+path(1)
            }
        }
        else {
            var words = rest.split(" ")
            var index = words.indexOf("Repo") 
            return words(index + 1)
        }
        return null
    }
    catch {
        case e: Exception => return null
    }
}
/*
def getUniqueRepoCount(rdd: org.apache.spark.rdd.RDD[LogEntry], client: String): Long = {
    return rdd.filter {
        x => x.rest != null &&
        x.download_id == client &&
        (x.rest.contains("Repo") || x.rest.contains("repos"))
    }.map(x => parseRepoName(x.rest))
    .filter(x => x != null)
    .distinct
    .count()
}*/



def invertedIndexBy(RDD : org.apache.spark.rdd.RDD[torrentSchema], field : String) = {
    RDD.groupBy(entry => 
        field match {
            case "debug_level" => entry.debug_level
            case "timestamp" => entry.timestamp
            case "download_id" => entry.download_id
            case "retrieval_stage" => entry.retrieval_stage
            case "rest" => entry.rest  
        }
    )
}



val filePath = "/home/radhika/Downloads/semester7/sdm/ghtorrent-logs.txt"

// Parsing data file
var Assignment_1 = parseFile(filePath)

val invertedRDD = invertedIndexBy(Assignment_1, "download_id") // create inverted index by download_id.


// b. Compute the number of different repositories accessed by the client ‘ghtorrent-22’ (without using the inverted index).
val client: String = "ghtorrent-22"

/*
val t1 = System.nanoTime
var numRepos: Long = Assignment_1.filter { row => 
                                                row.rest != null &&
                                                row.download_id == client &&
                                                (row.rest.contains("Repo") || 
                                                    row.rest.contains("repos"))
                                            }.map(row => 
                                                parseRepoName(row.rest)
                                            ).filter(repoName => 
                                                repoName != null).distinct.count

val duration = (System.nanoTime - t1) / 1e9d

//var numRepos: Long = getUniqueRepoCount(Assignment_1, client) // get number of unique repos.
println("\nThere are " + numRepos + " unique repositories for " + client + ".\n")
println("Time elapsed : " + duration + " seconds")
*/

// c. Compute the number of different repositories accessed by the client ‘ghtorrent-22’ using the inverted index calculated above.
val t1 = System.nanoTime
var numRepos: Long = invertedRDD.filter(x => x._1 == "ghtorrent-22").flatMap(x => x._2).filter(x => (x.rest.contains("Repo") || x.rest.contains("repos"))).map(x => parseRepoName(x.rest)).filter(x => x != null).distinct().count()
val duration = (System.nanoTime - t1) / 1e9d
println("\nThere are " + numRepos + " unique repositories for ghtorrent-22.\n")
println("Time elapsed : " + duration + " seconds")
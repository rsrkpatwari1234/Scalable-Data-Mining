/*import java.util.TimeZone
import java.text.SimpleDateFormat

case class my_schema (debug_level:String,timestamp:java.util.Date,download_id:String,retrieval_stage:String,rest:String)//

TimeZone.setDefault(TimeZone.getTimeZone("GMT"))
var timeFormat=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX") 

//to load an rdd according to the case class
def assignment_1(file_name:String):org.apache.spark.rdd.RDD[my_schema]={
	var file=sc.textFile(file_name)
	var parsedRDD=file.filter(x=>x.size>0).map(line=>line.split(", | -- ",4))
	parsedRDD=parsedRDD.map(line=> if(line.size==4)Array(line(0),line(1),line(2))++line(3).split(": ",2) else line)
	var myRDD=parsedRDD.map(x=>try{my_schema(x(0),timeFormat.parse(x(1)),x(2),x(3),x(4))}catch{case e:Exception=>my_schema(null,null,null,null,null)})
	return myRDD
}


// function to get the repos name
def repo_names(x:String):String={
	try{
		if(x.contains("github.com/repos/")){
			var words=x.split("github.com/repos/")(1).split("\\?")(0)
			var path=words.split("/")
			if(path.size==1){
				return path(0)
			}
			else if(path.size>1){
				return path(0)+"/"+path(1)
			}
		}
		else{
			var words=x.split(" ")
			var index=words.indexOf("Repo") 
			return words(index+1)
		}
		return null
	}
	catch{
		case e:Exception=>return null
	}
}

// function to get invert index rdd
def invertIndex(rdd:org.apache.spark.rdd.RDD[my_schema],key: String) = {rdd.groupBy(r=>key match{
	case "debug_level" => r.debug_level
	case "timestamp" => r.timestamp
	case "download_id" => r.download_id
	case "retrieval_stage" => r.retrieval_stage
})}


// getting the query without inverted index
val myRDD=assignment_1("/home/radhika/Downloads/semester7/sdm/ghtorrent-logs.txt")

val t1 = System.nanoTime
val solution1=myRDD.filter(x=>x.rest!=null && x.download_id=="ghtorrent-22" && (x.rest.contains("Repo") || x.rest.contains("repos"))).map(x=>repo_names(x.rest)).filter(x=>x!=null).distinct().count()
val duration1 = (System.nanoTime - t1) / 1e9d
println("Unique repos with download id ghtorrent-22 without using inverted index:",solution1)
println("Time elapsed : " + duration1 + " seconds")

// getting the query using inverted index
val invert_index=invertIndex(myRDD,"download_id") // creating the inverted index

val t2 = System.nanoTime
val list_log=invert_index.filter(x=>x._1=="ghtorrent-22").flatMap(x=>x._2).collect()
val solution2=sc.parallelize(list_log).filter(x=>(x.rest.contains("Repo") || x.rest.contains("repos"))).map(x=>repo_names(x.rest)).filter(x=>x!=null).distinct().count()
val duration2 = (System.nanoTime - t2) / 1e9d
println("Unique repos with download id ghtorrent-22 using inverted index:",solution2)
		
*/


//------------------ TRying question 3
/*import java.util.TimeZone
import java.text.SimpleDateFormat
import itertools

object MovieLensRatings { // declaring an object (as only one instance will be used)
    // Create inverted index for rdd on column idx_id
    case class metadata_schema (
	debug_level:String,
	timestamp:String,
	//timestamp:java.util.Date,
	download_id:String,
	retrieval_stage:String,
	rest:String)


	TimeZone.setDefault(TimeZone.getTimeZone("GMT"))
	var timeFormat=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX") 

    // function to get the repos name
	/*def repo_names(x:String):String={
		try{
			if(x.contains("github.com/repos/")){
				var words=x.split("github.com/repos/")(1).split("\\?")(0)
				var path=words.split("/")
				if(path.size==1){
					return path(0)
				}
				else if(path.size>1){
					return path(0)+"/"+path(1)
				}
			}
			else{
				var words=x.split(" ")
				var index=words.indexOf("Repo") 
				return words(index+1)
			}
			return null
		}
		catch{
			case e:Exception=>return null
		}
	}*/

	// function to get invert index rdd
	def invertIndex(rdd:org.apache.spark.rdd.RDD[metadata_schema],key: String) = {
		rdd.groupBy(r => 
			key match {
				case "debug_level" => r.debug_level
				case "timestamp" => r.timestamp
				case "download_id" => r.download_id
				case "retrieval_stage" => r.retrieval_stage
			})
	}

	/*def myParse(line:String): String = {
	    var line1 = line.replace(" -- ", ", ")
	    line1 = line1.replace(".rb: ", ",")
	    line1 = line1.replace(", ghtorrent-", ", ")
	    return line1.split(", ", 4)
	}*/

	def parseRepos(x):
	    try {
	        split = x[4].split('/')[4:6]
	        joinedSplit = '/'.join(split)
	        result = joinedSplit.split('?')[0]
	    }
	    catch { 
	        result = ""
	     }
	    x.append(result)
	    return x

    def main(args: Array[String]): Unit = {
        val filePath = "/home/radhika/Downloads/semester7/sdm/ghtorrent-logs.txt" // ratings filepath.
        
        val dataFile = sc.textFile(filePath)

		/* for parsing rdd rows
		# Columns:
		# 0: debug level, 1: timestamp, 2: downloader id, 
		# 3: retrieval stage, 4: Action?*/
		var parsedRDD = dataFile
							.filter(x => x.size>0)
							.map(line => line.split(", | --",4))
							.filter(x => len(x) == 4)
							.map(line => Array(line(0),line(1),line(2))++line(3).split(": ",2))

		var Assignment_1 = parsedRDD
								.map(x =>
									try {
										//metadata_schema(x(0),timeFormat.parse(x(1)),x(2),x(3),x(4))
										metadata_schema(x(0),x(1),x(2),x(3),x(4))
									} catch {
										case e:Exception =>
											metadata_schema(null,null,null,null,null)
										}
									)
								.filter(x => 
									x.rest != null && 
									(x.debug_level == "DEBUG" || 
										x.debug_level == "INFO" || 
										x.debug_level == "WARN"))

		/*
		var Assignment_1 = dataFile
								.map(line => line.split(", -- :",4))
								.filter(x => len(x)==5)
								.map(parseRepos)
								.filter(x => x[5] != "")
								.map(x =>
									try {
										//metadata_schema(x(0),timeFormat.parse(x(1)),x(2),x(3),x(4))
										metadata_schema(x(0),x(1),x(2),x(3),x(4))
									} catch {
										case e:Exception =>
											metadata_schema(null,null,null,null,null)
										}
									)
								.filter(x => 
									x.rest != null && 
									(x.debug_level == "DEBUG" || 
										x.debug_level == "INFO" || 
										x.debug_level == "WARN"))
		*/
		// getting the query without inverted index

		val t1 = System.nanoTime
		/*val solution1 = Assignment_1
							.filter(x => 
								x.download_id=="ghtorrent-22" && 
								(x.rest.contains("Repo") || x.rest.contains("repos")))
							.map(x => 
								repo_names(x.rest))
							.filter(x => x != null)
							.distinct()
							.count()*/
		solution1 = Assignment_1.filter(lambda row: row[2] == "ghtorrent-22").groupBy(lambda x: x[5])
		val duration1 = (System.nanoTime - t1) / 1e9d
		print(user22repos.count())
		//println("Unique repos with download id ghtorrent-22 without using inverted index:" + solution1)
        println("Time elapsed : " + duration1 + " seconds")
		
		// getting the query using inverted index

		val t2 = System.nanoTime
		val invertedIndex = invertIndex(Assignment_1,"download_id") // creating the inverted index

		val list_log = invert_index
							.filter(x =>
								x._1 == "ghtorrent-22")
							.flatMap(x => x._2)
							.collect()

		val solution2 = list_log
							.filter(x => 
								(x.rest.contains("Repo") || x.rest.contains("repos")))
							.map(x => repo_names(x.rest))
							.filter(x => x != null)
							.distinct()
							.count()
		/*lookedUp22 = invertedIndex.lookup("22")
		entries22 = next(lookedUp22.__iter__())
		uniqueRepos = []
		for x in entries22:
		    if x[5] not in uniqueRepos:
		        uniqueRepos.append(x[5])*/
		
		val duration2 = (System.nanoTime - t2) / 1e9d
		print(len(uniqueRepos))
		//println("Unique repos with download id ghtorrent-22 using inverted index: " + solution2)
		println("Time elapsed : " + duration2 + " seconds")
    }
}
*/
// ------------- QUESTION 2
object MovieLensRatings { // declaring an object (as only one instance will be used)
    def loadFile(filename: String): org.apache.spark.rdd.RDD[String] = {
        /* 
            function to load RDD (of strings) from filename. 
            Input: filename as String.
            Output: RDD of String type.
        */
        return sc.textFile(filename)
    }

    def parseFile(ratings: org.apache.spark.rdd.RDD[String], token: String): org.apache.spark.rdd.RDD[Array[String]] = {
        /* 
            a) function to parse RDD. 
            Inputs: 
                RDD of String type.
                Split token (String), by which to split.
            Output: RDD of Array of String type.
        */
        return ratings.map(x => x.split(token))
    }

    def getComedyMoviesCount(movies: org.apache.spark.rdd.RDD[Array[String]]): Long = {
        /* 
            a) function to parse RDD. 
            Inputs: 
                RDD of String type.
                Split token (String), by which to split.
            Output: RDD of Array of String type.
        */
        return movies.filter(x => (x(2) == "Comedy")).map(x => x(0)).distinct.count
    }


    def comedyMovieWithMostRating(ratings: org.apache.spark.rdd.RDD[Array[String]], movies: org.apache.spark.rdd.RDD[Array[String]]): (String, Int) = {

    	val comedyMovies = movies.filter(x => x(2) == "Comedy").map(x => (x(0),x(1)))
    	val movieRatings = ratings.map(x => (x(1),x(2)))
    	val comedyMovieRatings = comedyMovies.join(movieRatings).map(x => (x._2._1, x._2._2.toInt)).reduceByKey(_+_).collect
        val maxVal = comedyMovieRatings.maxBy(x => x._2) // find max by number of rating given by the user.

        return maxVal
    }

    def getSpecificMovieUsers(ratings: org.apache.spark.rdd.RDD[Array[String]]): Long = {

    	//movie_IDs 2858, 356 and 2329
    	val usersForMovie2858 = ratings.filter(x => x(1) == "2858").map(x => x(0)).distinct()
    	val usersForMovie356 = ratings.filter(x => x(1) == "356").map(x => x(0)).distinct()
    	val usersForMovie2329 = ratings.filter(x => x(1) == "2329").map(x => x(0)).distinct()
    	val answer = usersForMovie2858.intersection(usersForMovie356).intersection(usersForMovie2329).count

    	// another way
    	// separate and count==3
    	/*val usersForMovie = ratings.filter(x => x(1) == "2858" || x(1) == "356" || x(1) == "2329").map(x => (x(0),x(1))).distinct()
    	val answer = usersForMovie.map(x => x._1).map(x => (x,1)).reduceByKey(_+_).filter(x => x._2 == 3).count()
		*/
        return answer
    }

   
	def getSpecificMovieUsersUsingInvertedIndex(ratings: org.apache.spark.rdd.RDD[(String,Iterable[String])]): Long = {

		val usersForMovie2858 = ratings.filter(x => x._1=="2858").flatMap{case(key, row) => row}.distinct()
    	val usersForMovie356 = ratings.filter(x => x._1=="356").flatMap{case(key, row) => row}.distinct()
    	val usersForMovie2329 = ratings.filter(x => x._1=="2329").flatMap{case(key, row) => row}.distinct()
    	val answer = usersForMovie2858.intersection(usersForMovie356).intersection(usersForMovie2329).count

        return answer
    }

    def main(args: Array[String]): Unit = {
        val filePath1 = "/home/radhika/Downloads/semester7/sdm/data/movies.dat" // ratings filepath.
        val filePath2 = "/home/radhika/Downloads/semester7/sdm/data/users.dat" // ratings filepath.
        val filePath3 = "/home/radhika/Downloads/semester7/sdm/data/ratings.dat"
        
        val sepToken: String = "::"

        val movies = loadFile(filePath1).map(_.split(sepToken))
        val users = loadFile(filePath2).map(_.split(sepToken))
        val ratings = loadFile(filePath3).map(_.split(sepToken))

        // a) Read the movies and users files into RDDs. How many records are there in each RDD
        val lineCount1 = movies.count() // get number of lines.
        println("Movies file has " + lineCount1 + " lines")

        val lineCount2 = users.count() // get number of lines.
        println("Users file has " + lineCount2 + " lines")

        // b) How many of the movies are a comedy?
        val comedyMoviesCount = getComedyMoviesCount(movies)
        println(comedyMoviesCount + " movies have Comedy genre")

        // c) Which comedy has the most ratings? Return the title and the number of rankings.
        val comedyMovieWithHighestRating = comedyMovieWithMostRating(ratings, movies)
        println("Movie " + comedyMovieWithHighestRating._1 + " has highest rating of " + comedyMovieWithHighestRating._2)

        // e) Compute the number of unique users that rated the movies with movie_IDs 2858, 356 and 2329
        val t1 = System.nanoTime
        val usersWithSpecificMovies = getSpecificMovieUsers(ratings)
        val duration = (System.nanoTime - t1) / 1e9d
        println(usersWithSpecificMovies + " distinct users have rated all 3 movies with movie_IDs 2858, 356 and 2329")
        println("Time elapsed : " + duration + " seconds")

        // f) Create an inverted index on ratings, field movie_ID
        //ratings.take(1).foreach(x => println(x(0) + " " + x(1)))
        val ratingsInvertedOnMovieId = ratings.map(x => (x(1),x(0))).groupByKey
        println("First Item after inversion : ")
        //println(ratingsInvertedOnMovieId.lookup("2858"))
      	/*ratingsInvertedOnMovieId.collect().take(1).foreach(a => {
        	println(a._1)
		    a._2.foreach(println)
		  })*/

        // g) Compute the number of unique users that rated the movies with movie_IDs 2858, 356 and 2329
        val t2 = System.nanoTime
        val usersWithSpecificMoviesUsingInvertedIndex = getSpecificMovieUsersUsingInvertedIndex(ratingsInvertedOnMovieId)
        val duration2 = (System.nanoTime - t2) / 1e9d
        println(usersWithSpecificMoviesUsingInvertedIndex + " distinct users have rated all 3 movies with movie_IDs 2858, 356 and 2329")
        println("Time elapsed : " + duration2 + " seconds")
    }
}

// ----- QUESTION 1 ---- 
/*
object MovieLensRatings { // declaring an object (as only one instance will be used)
    def loadFile(filename: String): org.apache.spark.rdd.RDD[String] = {
        /* 
            function to load RDD (of strings) from filename. 
            Input: filename as String.
            Output: RDD of String type.
        */
        return sc.textFile(filename)
    }

    def getLineCount(ratings: org.apache.spark.rdd.RDD[String]): Long = {
        /* 
            b) function to get line count. 
            Input: the RDD of Strings read from the ratings.dat file.
            Output: Double.
        */
        return ratings.count()
        //return ratings.map(x => 1).sum() // map each line to 1 and sum over the array formed.
    }
    
    def getMovieCount(parsedFile: org.apache.spark.rdd.RDD[Array[String]]): Long = {
        /* 
            c) function to get number of distinct movies. 
            Input: the parsed RDD of Array of Strings.
            Output: Double.
        */
       	return parsedFile.map(x => x(1)).distinct().count()
       //println("hello----------------------")
        //return parsedFile.map(x => x(1)).distinct.map(x => 1).sum() // movies is the 2nd column (index=1).
    }
	
    def getMostActiveUser(parsedFile: org.apache.spark.rdd.RDD[Array[String]]): (String, Int) = {
        /* 
            d) function to get userId and ratings of the user who has rated the most number of movies.
            Input: the parsed RDD of Array of Strings.
            Output: (String, Int).
        */
        /*
        val userIds = parsedFile.map(x => x(0)) // user ids.
        val user_count_map = userIds.groupBy(identity).mapValues(_.size) // user -> count map
        val maxVal = user_count_map.maxBy(x => x._2)
        return user_count_map.max*/
        val userIds = parsedFile.map(x => x(0)) // user ids.
        val userHistogram = userIds.map(x => (x, 1)).reduceByKey(_+_).collect
        val maxVal = userHistogram.maxBy(x => x._2) // find max by number of rating given by the user.
        
        return maxVal 
    }
    
    def getUserWithMost5StarRatings(parsedFile: org.apache.spark.rdd.RDD[Array[String]]): (String, Int) = {
        /* 
            e) function to get userId and ratings of the user who has rated the most number of movies with a '5' star rating.
            Input: the parsed RDD of Array of Strings.
            Output: (String, Int).
        */
        val userRatingsAndIds = parsedFile.map(x => (x(0), x(2))) // user ids and corresponding ratings.
        val histogramOf5StarRatings = userRatingsAndIds.map(x => if (x.2 == "5") (x._1, 1) else (x._1, 0)).reduceByKey(_+_).collect // map and reduce like part d) but use conditional statement to allocate 1 only when rating is '5' otherwise assign 0.
        val maxVal = histogramOf5StarRatings.maxBy(x => x._2) // get the user who has given the most '5' star ratings.
        val userId = maxVal._1 // get user id of the user who has given the most '5' star ratings.
        val userRatings = parsedFile.map(x => x(0)).map(x => if (x == userId) 1 else 0).reduce((x, y) => x+y) // count the total ratings give by the picked user id. (map to user column then map 1 when row has the user in question and then reduce by simple addition.)

        return (userId, userRatings)
    }

    def parseFile(ratings: org.apache.spark.rdd.RDD[String], token: String): org.apache.spark.rdd.RDD[Array[String]] = {
        /* 
            a) function to parse RDD. 
            Inputs: 
                RDD of String type.
                Split token (String), by which to split.
            Output: RDD of Array of String type.
        */
        return ratings.map(x => x.split(token))
    }

    def main(args: Array[String]): Unit = {
        val filePath = "/home/radhika/Downloads/semester7/sdm/data/ratings.dat" // ratings filepath.
        val ratings = loadFile(filePath) // load ratings RDD.
        // a) Download the ratings file, parse it and load it in an RDD named ratings.
        val sepToken: String = "::"
        val parsedFile = parseFile(ratings, sepToken) 
        // b) How many lines does the ratings RDD contain?
        val lineCount = getLineCount(ratings) // get number of lines.
        println("Ratings file has " + lineCount + " lines")
        // c) Count how many unique movies have been rated. 
        val movieCount = getMovieCount(parsedFile) //
        println("Ratings file has " + movieCount + " distinct movies")
        // d) Which user gave the most ratings? Return the userID and number of ratings.
        val mostActiveUser = getMostActiveUser(parsedFile) // get the user who has rated the most movies.
        var userId = mostActiveUser._1 // get first return value (userId)
        var moviesRated = mostActiveUser._2 // get second return value (number of movies rated by the user with id=userId)
        println("user with id: " + userId + " has rated " + moviesRated + " movies, which is the highest.")
        // e) Which user gave the most '5' ratings? Return the userID and number of ratings.
        val userWithMost5StarRatings = getUserWithMost5StarRatings(parsedFile) // get th user who has given most '5' star ratings to movies.
        userId = userWithMost5StarRatings._1 // get first return value (userId)
        moviesRated = userWithMost5StarRatings._2 // get second return value (number of movies rated by the user with id=userId)
        println("user with id: " + userId + " (who has rated " + moviesRated + " movies) has given most '5' star ratings.")
    }
}
// invoke main method of object to get output
//MovieLensRatings.main(Array(""))
*/

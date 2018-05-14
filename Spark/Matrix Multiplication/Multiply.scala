package edu.uta.cse6331

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Multiply {
  def main(args: Array[ String ]) {
  
    val conf = new SparkConf().setAppName("Multiply")
    
    val sc = new SparkContext(conf)
    
    val m = sc.textFile(args(0)).map( line => { val a = line.split(",")
                                    (a(0).toInt,a(1).toInt,a(2).toDouble) } )
                                                
    val M = m.map({ case (i, j, v) => (j->(0,i,v))}) 
     
    val n = sc.textFile(args(1)).map( line => { val a = line.split(",")
									(a(0).toInt,a(1).toInt,a(2).toDouble) }) 
    
    val N = n.map({ case (i, j, v) => (i->(1,j,v)) })
     
    val result1 = M.map(M=>M).join(N.map(N=>N)).map{case (k,((a,b,c),(x,y,z))) => (b,y)->(c*z)}

    val res=result1.reduceByKey((k, n) => k+ n).sortByKey().map{case ((a,b),c)=>a+","+b+","+c}.repartition(1)
  
    res.saveAsTextFile(args(2))
  
    sc.stop()
  }
}

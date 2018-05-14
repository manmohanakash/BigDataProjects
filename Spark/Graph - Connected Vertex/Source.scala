package edu.uta.cse6331

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Source {
  def main(args: Array[ String ]) {
    
	val conf = new SparkConf().setAppName("Multiply")
    val sc = new SparkContext(conf)
    
    var graph=sc.textFile(args(0)).map( line => { val a = line.split(",")
									  (a(0).toLong,a(1).toLong,a(2).toLong) } )
    						
    var graph_set = graph.map({ case (a, b, c) => (c->(a,b)) }).groupByKey()
    var graph_init = graph_set.map({ case (a, b) => if (a==0.toLong)(a,0.toLong,b) else (a,Long.MaxValue,b)  })

    var vertex=sc.broadcast(graph_init.map({ case m => (m._1,m._2) }).collect)
    var graph_fetch= graph_init.map({ case (a,b,c) => (a,b,c.map({ case (x,y) => if(vertex.value.exists(_._1==x)==true) (vertex.value.filter(_._1==x)(0).toString.split(",")(1).dropRight(1).toLong,y) else (Long.MaxValue,y) }) ) }) 
    var graph_update=graph_fetch.map({ case (a,b,c) => (a,b,c.map{ case (a, b) => if(a!=Long.MaxValue)(a+b) else (a) }) }).map({ case (a,b,c) => (a,b,c.min) }).map({ case (a,b,c) => if(b<=c)(a,b) else (a,c) })
    vertex.unpersist
    
    for (i <- 0 to 2){
      vertex =sc.broadcast(graph_update.collect)
      var node_pair= graph_update.map({case (x,y)=> x->y})
      var edge_pair= graph_init.map({case (x,y,z)=>x->z})
      var graph_updated_set = node_pair.map(M=>M).join(edge_pair.map(N=>N)).map{case (k,(a,y)) => (k,a,y)}
      graph_fetch= graph_updated_set.map({ case (a,b,c) => (a,b,c.map({ case (x,y) => if(vertex.value.exists(_._1==x)==true) (vertex.value.filter(_._1==x)(0).toString.split(",")(1).dropRight(1).toLong,y) else (Long.MaxValue,y) }) ) }) 
      graph_update=graph_fetch.map({ case (a,b,c) => (a,b,c.map{ case (a, b) => if(a!=Long.MaxValue)(a+b) else (a) }) }).map({ case (a,b,c) => (a,b,c.min) }).map({ case (a,b,c) => if(b<=c)(a,b) else (a,c) })
      vertex.unpersist 
    }
    var filter=graph_update.filter(_._2!=Long.MaxValue)
    var print=filter.repartition(1).sortBy(_._1).map({ case (a,b) => a+" "+b })
    print.saveAsTextFile(args(1))
    }
}

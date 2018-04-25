package de.tuberlin.dima.bdapro.solutions.gameoflife;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.*;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import  org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.operators.Order;






public class GameOfLifeTaskImpl implements GameOfLifeTask {


	@Override
	public int solve(String inputFile, int argN, int argM, int numSteps) {

		//******************************
		//*Implement your solution here*

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<String> AliveCell = env.fromElements(
				"1 2\n" +
						"2 2\n" +
						"3 2\n" +
						"7 7\n" +
						"7 8\n" +
						"8 7\n" +
						"8 8\n" +
						"9 9\n" +
						"9 10\n" +
						"10 9\n" +
						"10 10"
		);

/*
		DataSet<String> AliveCell = env.readTextFile(inputFile);*/

		final int xsize = argN;  // takes board size off command line
		final int ysize = argM;


		class LineSplitter implements FlatMapFunction<String, Tuple2<Cell,Integer>> {

			@Override
			public void flatMap(String value, Collector<Tuple2<Cell,Integer>> out) {
				String[] points = value.split("\n");

				for (String point : points) {
					if (point.length() > 0) {
						String[] cors = point.split("\\W+");
						Cell c = new Cell(Integer.valueOf(cors[0]), Integer.valueOf(cors[1]));
						out.collect(new Tuple2<Cell,Integer>(c,1));
					}
				}
			}
		}


		 class GetNeighboreEdges implements FlatMapFunction<Tuple2<Cell,Integer>, Tuple3< Cell,Cell,NullValue>> {

			@Override
			public void flatMap(Tuple2<Cell,Integer> cell, Collector<Tuple3< Cell,Cell,NullValue>> out)
			{

				int x=cell.f0.getX();
				int y=cell.f0.getY();
				Cell sourceCell = new Cell(cell.f0.getX(),cell.f0.getY());

				Cell NeighborCell1 = new Cell(mod(x+1,xsize),y);
				Cell NeighborCell2 = new Cell(mod(x+1,xsize),mod(y+1, ysize));
				Cell NeighborCell3 = new Cell(x,mod(y+1,ysize));
				Cell NeighborCell4 = new Cell(x,mod(y-1,ysize));
				Cell NeighborCell5 = new Cell(mod(x+1,xsize),mod(y-1,ysize));
				Cell NeighborCell6 = new Cell(mod(x-1,xsize),y);
				Cell NeighborCell7 = new Cell(mod(x-1,xsize),mod(y-1,ysize));
				Cell NeighborCell8 = new Cell(mod(x-1,xsize),mod(y+1,ysize));


				out.collect(new Tuple3< Cell,Cell,NullValue>(sourceCell,NeighborCell1,NullValue.getInstance()));
				out.collect(new Tuple3< Cell,Cell,NullValue>(sourceCell,NeighborCell2,NullValue.getInstance()));
				out.collect(new Tuple3< Cell,Cell,NullValue>(sourceCell,NeighborCell3,NullValue.getInstance()));
				out.collect(new Tuple3< Cell,Cell,NullValue>(sourceCell,NeighborCell4,NullValue.getInstance()));
				out.collect(new Tuple3< Cell,Cell,NullValue>(sourceCell,NeighborCell5,NullValue.getInstance()));
				out.collect(new Tuple3< Cell,Cell,NullValue>(sourceCell,NeighborCell6,NullValue.getInstance()));
				out.collect(new Tuple3< Cell,Cell,NullValue>(sourceCell,NeighborCell7,NullValue.getInstance()));
				out.collect(new Tuple3< Cell,Cell,NullValue>(sourceCell,NeighborCell8,NullValue.getInstance()));
			}

			 private   int mod (int x, int m){
				 return (x % m + m) % m;
			 }
		}
		DataSet<Tuple2<Cell,Integer>> cells = AliveCell.flatMap(new LineSplitter());



		IterativeDataSet<Tuple2<Cell,Integer>> loop = cells.iterate(numSteps);
		DataSet<Tuple3< Cell,Cell,NullValue>> edges = loop.flatMap(new GetNeighboreEdges());
		loop.sortPartition("f0.id", Order.ASCENDING);


		DataSet<Tuple2<Cell,Integer>> vertexTuples = edges.distinct("f1.x","f1.y").coGroup(loop)
				.where("f1.x","f1.y")
				.equalTo("f0.x","f0.y")
				.with(new CoGroupFunction<Tuple3<Cell,Cell,NullValue>, Tuple2< Cell,Integer>, Tuple2<Cell,Integer>>(){

					public void coGroup(Iterable<Tuple3< Cell,Cell,NullValue>> edges,
										Iterable<Tuple2< Cell,Integer>> aliveCells,
										Collector<Tuple2< Cell,Integer>> deadNAlive) throws Exception {



						for (Tuple3< Cell,Cell,NullValue> edge : edges) {
							if(edge.f1.getX()==8&&edge.f1.getY()==8){
							}
							boolean hadElements = false;

							try {
								for (Tuple2< Cell,Integer> rightElem : aliveCells) {
									hadElements = true;
									break;
								}
							} catch (Exception e) {
								e.printStackTrace();
							}
							if (!hadElements) {
								if(edge.f1.getX()==8&&edge.f1.getY()==8){
								}
								deadNAlive.collect(new Tuple2< Cell,Integer>(edge.f1,0));
							}
						}

					}
				})
				.distinct("f0.x","f0.y");
		vertexTuples = loop.union(vertexTuples);

		DataSet<Tuple3<Cell, Cell, NullValue>> edgeTuples = edges;
		Graph<Cell, Integer, NullValue> graph = Graph.fromTupleDataSet(vertexTuples, edgeTuples, env);
		graph = graph.getUndirected();

		DataSet<Edge<Cell, NullValue>> edges3 = graph.getEdges();
		DataSet<Tuple2<Cell, Integer>> verticesWithSum2 = edges3.leftOuterJoin(vertexTuples)
				.where("f1.x","f1.y").equalTo("f0.x","f0.y")
				.with(new JoinFunction<Edge<Cell, NullValue>, Tuple2<Cell,Integer>, Tuple3<Cell,Cell,Integer>>() {
					public Tuple3<Cell,Cell,Integer> join(Edge<Cell, NullValue> v1, Tuple2<Cell,Integer> v2) {

						if (null==v2){
							return new Tuple3<Cell,Cell,Integer>(v1.getSource(),v1.getTarget(),0);
						}
						return new Tuple3<Cell,Cell,Integer>(v1.getSource(),v1.getTarget(),v2.f1);

					}}).distinct().map(new MapFunction<Tuple3<Cell,Cell,Integer>, Tuple2<Cell,Integer>>() {
					@Override
					public Tuple2<Cell,Integer> map(Tuple3<Cell,Cell,Integer> v) throws Exception {


						return new Tuple2<Cell,Integer>(v.f0,v.f2);
					}
				} ).groupBy("f0.x","f0.y").sum(1);


		DataSet<Tuple2<Cell, Integer>> nextGenCells = graph.getVertices().map(new MapFunction<Vertex<Cell, Integer>, Tuple2<Cell, Integer>>() {
			public  Tuple2<Cell, Integer> map(Vertex<Cell, Integer> value) {
				return new Tuple2<Cell, Integer>(value.f0,value.f1); }
		}).leftOuterJoin(verticesWithSum2).where("f0.x","f0.y").equalTo("f0.x","f0.y")
				.with(new JoinFunction<Tuple2<Cell,Integer>, Tuple2<Cell,Integer>, Tuple2<Cell,Integer>>() {
					public Tuple2<Cell,Integer> join(Tuple2<Cell,Integer> v1, Tuple2<Cell,Integer> v2) {

						if (null==v2){
							return new Tuple2<Cell,Integer>(v1.f0,0);
						}

						if ((v1.f1==1 && v2.f1==2)||v2.f1==3){
							return new Tuple2<Cell,Integer>(v1.f0,1);
						}else {
							return new Tuple2<Cell,Integer>(v1.f0,0);
						}


					}});


		nextGenCells = nextGenCells.filter(new FilterFunction<Tuple2<Cell, Integer>>() {
			public boolean filter(Tuple2<Cell, Integer> value) { return value.f1 > 0; }
		});

		DataSet<Tuple2<Cell, Integer>> finalAliveCells = loop.closeWith(nextGenCells);


		long res = 0;
		try {
			res =  finalAliveCells.count();
		} catch (Exception e) {
			e.printStackTrace();
		}
		//System.out.println("finalAliveCells  "+res);
		return (int)res;




	}


}
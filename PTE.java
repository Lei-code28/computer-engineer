import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.util.LongAccumulator;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

//对于一个A型wedge，在不扩展的情形的，读取外存中存储的V型wedge构造出边，然后写入外存
//参数partitionNumForTypeV 用于对V型Wedge进行取模分类
//参数colors 用于对V型Wedge取模分类，然后计算出所有的边，同时也是后续的分块的n
public class PTE {

//    private static String inputHdfsPath = "hdfs://Master:9000/chapter4/input/";
//
//    private static String outputHdfsPath = "/chapter4/experiment/";

    private static final String DELIMITER = " ";

    public static void main(String[] args) throws IOException {

        //TODO Step0 环境设置
        SparkConf sparkConf = new SparkConf()
                .setAppName(
                            "Pte-Bi-triangle" + "-"   +  args[6] + "-coresMax-"   +  args[0] + "-exCores-"  + args[1] + "-exMemory-"    +  args[2]  +
                            "-memoryF-"   +  args[3] + "-memorySF-" + args[4] + "_defaultPara_" +  args[5]  +
                                    "-numA-"    + args[7] + "-numV-"       +  args[8])

                .set("spark.cores.max", args[0])
                .set("spark.executor.cores", args[1])
                .set("spark.executor.memory", args[2])

                .set("spark.memory.fraction",args[3])
                .set("spark.memory.storageFraction",args[4])

                .set("spark.default.parallelism", args[5])

                .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.registrationRequired","true")
                .set("spark.kryoserializer.buffer", "24m")
                .registerKryoClasses(new Class[]{Tuple2.class, ArrayList.class, LinkedList.class, HashSet.class});

        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");

        String dataName = args[6];
        Integer partitionNumForTypeA = Integer.parseInt(args[7]);
        Integer partitionNumForTypeV = Integer.parseInt(args[8]);

        Integer modS = Integer.parseInt(args[9]);
        Integer modE = Integer.parseInt(args[10]);

//        String inputHdfsPath = "/chapter4/input/";
        String inputHdfsPath = "hdfs://10.176.24.42:9000/chapter4/input/";
        String outputHdfsPath = "/chapter4/experiment/pte";

        String path1ForTypeVStorage = outputHdfsPath + "/pte-"     +dataName +"-numA"+ args[7] +"-numV"+args[8] + "-defaultP"+args[5]+"/" + "WedgeTypeV";
        String path2ForTotalEdgeStorage = outputHdfsPath + "/pte-" +dataName +"-numA"+ args[7] +"-numV"+args[8] + "-defaultP"+args[5]+"/" + "EdgeNoClassify";
        String path3ForSplitEdgeStorage = outputHdfsPath + "/pte-" +dataName +"-numA"+ args[7] +"-numV"+args[8] + "-defaultP"+args[5]+"/" + "EdgeSplit";
        String path4ForResult = outputHdfsPath + "/pte-"           +dataName +"-numA"+ args[7] +"-numV"+args[8] + "-defaultP"+args[5]+"/" + "Result"  ;

        //TODO Step1 创建存储edgeSet的hdfs文件夹和读取初始数据
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(new URI("hdfs://10.176.24.42:9000"), conf, "star");

            fs.mkdirs(new Path(path1ForTypeVStorage));
            fs.mkdirs(new Path(path2ForTotalEdgeStorage));
            fs.mkdirs(new Path(path3ForSplitEdgeStorage));

        } catch (IOException | URISyntaxException | InterruptedException e) {
            e.printStackTrace();
        }

        LongAccumulator totalCount = sc.sc().longAccumulator();

        JavaRDD<String> inputData = sc.textFile(inputHdfsPath + dataName);

        //TODO Step2 构造出V型wedge，并将其写入外存
        JavaPairRDD<Integer, HashSet<Integer>> wedgeTypeV = inputData.mapPartitionsToPair(new readEdgesForWedgeTypeV()).combineByKey(new CreateCombinerMv(), new MergeValueMv(), new MergeCombinerMv());
        wedgeTypeV.partitionBy(new wedgeVToHdfsPartitioner(partitionNumForTypeV)).foreachPartition(it -> {
            if (it.hasNext()) {
                Configuration conf = new Configuration();
                FileSystem fs = FileSystem.get(new URI("hdfs://10.176.24.42:9000"), conf, "star");

                Path path1 = new Path(path1ForTypeVStorage + "/V-" + TaskContext.getPartitionId());
                Output output = new Output(new SnappyOutputStream(fs.create(path1).getWrappedStream()));
                while (it.hasNext()) {
                    Tuple2<Integer, HashSet<Integer>> entry = it.next();
                    Integer mVertex = entry._1;
                    HashSet<Integer> neighbours = entry._2;
                    output.writeInt(mVertex, true);
                    output.writeInt(neighbours.size(), true);
                    for (Integer i : neighbours) {
                        output.writeInt(i, true);
                    }
                }
                output.writeInt(-1, true);
                output.close();
            }
        });

        JavaPairRDD<Integer, HashSet<Integer>> wedgeTypeA = inputData.mapPartitionsToPair(new readEdgesForWedgeTypeA()).combineByKey(new CreateCombinerMv(), new MergeValueMv(), new MergeCombinerMv());

        //TODO Step3 构造边——A型wedge根据拐点分类之后，读取写入外存的V型wedge构造出所有的边然后，将边写入外存
        wedgeTypeA.partitionBy(new wedgeAMiddleVetextPartitioner(partitionNumForTypeA)).foreachPartition(it -> {
            if (it.hasNext()) {
                Configuration conf = new Configuration();
                FileSystem fs = FileSystem.get(new URI("hdfs://10.176.24.42:9000"), conf, "star");

                HashSet<Tuple2<Integer,HashSet<Integer>>> setForTypeA = new HashSet<>();
                HashMap<Integer,HashSet<Integer>> mapForTypeV = new HashMap<>();

                HashSet<Integer> fNrKeys = new HashSet<>();

                //遍历所有的一阶邻居，求出需要读取块的index
                while(it.hasNext()) {
                    Tuple2<Integer, HashSet<Integer>> singleWedgeA = it.next();
                    setForTypeA.add(singleWedgeA);

                    HashSet<Integer> fNrs = singleWedgeA._2;
                    for (Integer n : fNrs) {
                        int key = n % partitionNumForTypeV;
                        fNrKeys.add(key);
                    }
                }

                //读取V型wedge,存入mapForTypeA
                for (Integer fNr : fNrKeys) {
                    Input in = new Input(new SnappyInputStream(fs.open(new Path( path1ForTypeVStorage +  "/V-" + fNr)).getWrappedStream()));
                    while (true) {
                        int mV = in.readInt(true);
                        if (mV == -1) {
                            break;
                        }
                        int nSize = in.readInt(true);
                        HashSet<Integer> neighbours = new HashSet<>();
                        for (int i = 1; i <= nSize; i++) {
                            int n = in.readInt(true);
                            neighbours.add(n);
                        }

                        if (mapForTypeV.containsKey(mV)) {
                            HashSet<Integer> temp1 = mapForTypeV.get(mV);
                            if (neighbours.size() < temp1.size()) {
                                temp1.addAll(neighbours);
                                mapForTypeV.put(mV,temp1);
                            }
                            else  {
                                neighbours.addAll(temp1);
                                mapForTypeV.put(mV,temp1);
                            }
                        } else  {
                            mapForTypeV.put(mV,neighbours);
                        }

                    }
                    in.close();
                }
                fNrKeys.clear();

                //遍历setForTypeA，读取块，构造边
                Path path2 = new Path(path2ForTotalEdgeStorage + "/edgeStartBy-" + TaskContext.getPartitionId());
                Output output = new Output(new SnappyOutputStream(fs.create(path2).getWrappedStream()));
                for (Tuple2<Integer,HashSet<Integer>> item : setForTypeA) {
                    Integer u = item._1;
                    HashSet<Integer> fNeighbours = item._2;
                    for (Integer w : fNeighbours) {
                        if (mapForTypeV.containsKey(w)) {
                            HashSet<Integer> sHopNrSet = mapForTypeV.get(w); //集群测试的时候在这一步查找不到
                            for (Integer v : sHopNrSet) {
                                if (v <= u)
                                    continue;
                                //TODO write （u,v,w）
                                output.writeInt(u,true);
                                output.writeInt(v,true);
                                output.writeInt(w,true);
                            }
                        }
                    }
                }
                output.writeInt(-1,true);
                output.close();
            }
        });

        //TODO Step4 构造组合数, 通过if判断条件对边进行分类
        int edgeBlockKey = 0;
        ArrayList<Tuple3<Integer,Integer,Integer>> edgeBlocks= new ArrayList<>();

        for(int i = 0; i <= partitionNumForTypeA -1; i++) {
            for (int j = i; j <= partitionNumForTypeA -1; j++) {
                edgeBlocks.add(new Tuple3<>(i, j, edgeBlockKey++));
            }
        }

        JavaPairRDD<Integer, Tuple2<Integer, Integer>> edgeBlockPartition = sc.parallelize(edgeBlocks).mapPartitionsToPair(new EdgeBlockToPairRDD())
                .partitionBy(new EdgeBlockPartitioner(edgeBlockKey));

        edgeBlockPartition.foreachPartition(it -> {
            while (it.hasNext()) {
                Configuration conf = new Configuration();
                FileSystem fs = FileSystem.get(new URI("hdfs://10.176.24.42:9000"), conf, "star");
                Tuple2<Integer, Tuple2<Integer, Integer>> temp = it.next();

                Tuple2<Integer, Integer> bValue = temp._2;

                Integer color1= bValue._1;
                Integer color2= bValue._2;


                if (color1  == color2 ) {

                    Path path2_1_1 = new Path(path3ForSplitEdgeStorage + "/edge-" + color1 + "-" + color1);
                    Output out_1_1 = new Output(new SnappyOutputStream(fs.create(path2_1_1).getWrappedStream()));


                    String tempPath = path2ForTotalEdgeStorage + "/edgeStartBy-" + color1;
                    Path path = new Path(tempPath);
                    if (!fs.exists(path)) continue;
                    Input in = new Input(new SnappyInputStream(fs.open(path).getWrappedStream()));
                    while (true) {
                        int u = in.readInt(true);
                        if (u == -1) {
                            break;
                        }
                        int v = in.readInt(true);
                        int w = in.readInt(true);

                        int u_local = u / partitionNumForTypeA;
                        int v_local = v / partitionNumForTypeA;
                        int u_color = (u - u_local * partitionNumForTypeA);
                        int v_color = (v - v_local * partitionNumForTypeA);

                        if (u_color == color1 && v_color == color1) {
                            out_1_1.writeInt(u,true);
                            out_1_1.writeInt(v,true);
                            out_1_1.writeInt(w,true);
                        }
                        //                        System.out.println("key=" + ke + "," + "u=" + u + "v=" + v + "w=" + w + "partitionId=" + TaskContext.getPartitionId());
                    }
                    in.close();

                    out_1_1.writeInt(-1, true);
                    out_1_1.close();
                }
                else if (color1 != color2) {

                    Path path2_1_2 = new Path(path3ForSplitEdgeStorage + "/edge-" + color1 + "-" + color2);
                    Output out_1_2 = new Output(new SnappyOutputStream(fs.create(path2_1_2).getWrappedStream()));

                    String tempPath1 = path2ForTotalEdgeStorage + "/edgeStartBy-" + color1;
                    Path path1 = new Path(tempPath1);
                    if (!fs.exists(path1)) continue;
                    Input in1 = new Input(new SnappyInputStream(fs.open(path1).getWrappedStream()));
                    while (true) {
                        int u = in1.readInt(true);
                        if (u == -1) {
                            break;
                        }
                        int v = in1.readInt(true);
                        int w = in1.readInt(true);

                        int u_local = u / partitionNumForTypeA;
                        int v_local = v / partitionNumForTypeA;
                        int u_color = (u - u_local * partitionNumForTypeA);
                        int v_color = (v - v_local * partitionNumForTypeA);


                        if (u_color == color1 && v_color == color2) {
                            out_1_2.writeInt(u,true);
                            out_1_2.writeInt(v,true);
                            out_1_2.writeInt(w,true);
                        }
                        //                        System.out.println("key=" + ke + "," + "u=" + u + "v=" + v + "w=" + w + "partitionId=" + TaskContext.getPartitionId());
                    }
                    in1.close();
                    out_1_2.writeInt(-1, true);
                    out_1_2.close();



                    Path path2_2_1 = new Path(path3ForSplitEdgeStorage + "/edge-" + color2 + "-" + color1);
                    Output out_2_1 = new Output(new SnappyOutputStream(fs.create(path2_2_1).getWrappedStream()));

                    String tempPath2 = path2ForTotalEdgeStorage + "/edgeStartBy-" + color2;
                    Path path2 = new Path(tempPath2);
                    if (!fs.exists(path2)) continue;
                    Input in2 = new Input(new SnappyInputStream(fs.open(path2).getWrappedStream()));
                    while (true) {
                        int u = in2.readInt(true);
                        if (u == -1) {
                            break;
                        }
                        int v = in2.readInt(true);
                        int w = in2.readInt(true);

                        int u_local = u / partitionNumForTypeA;
                        int v_local = v / partitionNumForTypeA;
                        int u_color = (u - u_local * partitionNumForTypeA);
                        int v_color = (v - v_local * partitionNumForTypeA);

                        if (u_color == color2 && v_color == color1) {
                            out_2_1.writeInt(u,true);
                            out_2_1.writeInt(v,true);
                            out_2_1.writeInt(w,true);
                        }
                        //                        System.out.println("key=" + ke + "," + "u=" + u + "v=" + v + "w=" + w + "partitionId=" + TaskContext.getPartitionId());
                    }
                    in2.close();

                    out_2_1.writeInt(-1, true);
                    out_2_1.close();
                }
            }
        });

        //TODO Step5 构造组合数, 读取边进行计算
        int problemKey = 0;

        ArrayList<Tuple4<Integer,Integer,Integer,Integer>> problems= new ArrayList<>(); //第三位设置为-2，表示是第二类组合数 第四位是组合数的id

        for(int i = 0; i <= partitionNumForTypeA -2; i++) {
            for (int j = i + 1; j <= partitionNumForTypeA -1; j++) {
                int type = -2;
                problems.add(new Tuple4<>(i, j, type,problemKey++));

                if (i > partitionNumForTypeA -3) continue;
                for (int k = j+1; k<= partitionNumForTypeA -1; k++) {
                    problems.add(new Tuple4<>(i, j, k,problemKey++));
                }
            }
        }

        JavaPairRDD<Integer, Tuple3<Integer,Integer,Integer>> problemsPartition = sc.parallelize(problems).mapPartitionsToPair(new ProblemsToPairRDD())
                .partitionBy(new ProblemsPartitioner(problemKey));

        problemsPartition.foreachPartition(it -> {
            while (it.hasNext()) {
                Long biCount = 0L;
                Configuration conf = new Configuration();
                FileSystem fs = FileSystem.get(new URI("hdfs://10.176.24.42:9000"), conf, "star");

                Tuple2<Integer, Tuple3<Integer, Integer, Integer>> item = it.next();
                Tuple3<Integer, Integer, Integer> pValue = item._2;

                Integer color1 = pValue._1();
                Integer color2 = pValue._2();
                Integer color3 = pValue._3();


                if (color3 == -2) {
                    ProblemFor2Colors problem2 = new ProblemFor2Colors(partitionNumForTypeA,color1,color2,fs,path3ForSplitEdgeStorage);
                    biCount += problem2.triangleNum();
                } else {
                    ProblemFor3Colors problem3 = new ProblemFor3Colors(color1,color2,color3,fs,path3ForSplitEdgeStorage);
                    biCount+=problem3.triangleNum();
                }


                if (biCount != 0) {
                    totalCount.add(biCount);
//                    if (color3 == -2) {
//                        String path4 = path4ForResult + "/" + "colors=(" + color1 + ","+ color2 + ")-" + TaskContext.getPartitionId() + "-" + biCount;
//                        fs.mkdirs(new Path(path4));
//                    }else {
//                        String path4 = path4ForResult + "/" + "colors=(" + color1 + ","+ color2 + "," + color3 + ")-" + TaskContext.getPartitionId() + "-" + biCount;
//                        fs.mkdirs(new Path(path4));
//                    }
                }
            }
        });





        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(new URI("hdfs://10.176.24.42:9000"), conf, "star");
            String path4 = path4ForResult + "/total_bi-triangle_num =" + totalCount.value();
            fs.mkdirs(new Path(path4));

        } catch (IOException | URISyntaxException | InterruptedException e) { e.printStackTrace(); }

        sc.stop();
    }


    //用于problem分类
    private static class ProblemsToPairRDD implements PairFlatMapFunction<Iterator<Tuple4<Integer,Integer,Integer,Integer>>, Integer, Tuple3<Integer,Integer,Integer>> {
        @Override
        public Iterator<Tuple2<Integer, Tuple3<Integer,Integer,Integer>>> call(Iterator<Tuple4<Integer,Integer,Integer,Integer>> in) throws Exception {
            ArrayList<Tuple2<Integer, Tuple3<Integer,Integer,Integer>>> result = new ArrayList<>();
            while (in.hasNext()) {
                Tuple4<Integer,Integer,Integer, Integer> problem = in.next();

                Integer i = problem._1();
                Integer j = problem._2();
                Integer type = problem._3();
                Integer key = problem._4();


                result.add(new Tuple2<>(key,new Tuple3<>(i,j,type)));
            }
            return result.iterator();
        }
    }
    private static class ProblemsPartitioner extends Partitioner {

        private int numPartition;

        public int getNumPartition() {
            return numPartition;
        }

        public ProblemsPartitioner(int numPartition) {
            this.numPartition = numPartition;
        }

        @Override
        public int numPartitions() {
            return getNumPartition();
        }

        @Override
        public int getPartition(Object key) {

            int partitionId = (int) key;

            return partitionId;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof ProblemsPartitioner) {
                ProblemsPartitioner partitionerObject = (ProblemsPartitioner) obj;
                if (partitionerObject.getNumPartition() == this.getNumPartition())
                    return true;
            }
            return false;
        }
    }

    //计算
    private static class ProblemFor3Colors {
        private Integer color0;
        private Integer color1;
        private Integer color2;

        private Map<Integer, HashSet<Integer>> map;
        private Map<Tuple2<Integer,Integer>,HashSet<Integer>> edgeProjectedWeight;
        private Long triangleCount;

        private FileSystem fs;

        private String dataPathSuffix;


        public ProblemFor3Colors(Integer color1, Integer color2, Integer color3, FileSystem fs, String dataPathSuffix) {
            this.color0 = color1;
            this.color1 = color2;
            this.color2 = color3;

            this.map = new HashMap<>();
            this.edgeProjectedWeight = new HashMap<>();
            this.triangleCount = 0L;
            this.fs = fs;
            this.dataPathSuffix = dataPathSuffix;
        }

        public void readEdgeSet (Integer uColor, Integer vColor) throws IOException, URISyntaxException, InterruptedException {
            //TODO 读取数据6次然后存入hashMap中,比如颜色012，读取01，10，02，20，12，21

            Path edgeSetPath = new Path(dataPathSuffix + "/edge-" + uColor +"-" + vColor );

            if (!fs.exists(edgeSetPath)) {
                System.out.println("no exsist edge path");
                return;
            }

            Input in = new Input(new SnappyInputStream(fs.open(edgeSetPath).getWrappedStream()));
            while (true) {
                int u = in.readInt(true);
                if (u == -1) {
                    break;
                }
                int v = in.readInt(true);

                int w = in.readInt(true);

                if (map.containsKey(u)) {
                    HashSet<Integer> neighbours = map.get(u);
                    neighbours.add(v);
                    map.put(u,neighbours);
                } else {
                    HashSet<Integer> neighbours = new HashSet<>();
                    neighbours.add(v);
                    map.put(u,neighbours);
                }

                Tuple2<Integer,Integer> edge = new Tuple2<>(u,v);

                if (edgeProjectedWeight.containsKey(edge)) {
                    HashSet<Integer> weights = edgeProjectedWeight.get(edge);
                    weights.add(w);
                    edgeProjectedWeight.put(edge,weights);
                } else {
                    HashSet<Integer> weights = new HashSet<>();
                    weights.add(w);
                    edgeProjectedWeight.put(edge,weights);
                }
            }
            in.close();
        }

        public void buildGraphPartition() throws IOException, URISyntaxException, InterruptedException {
            //读取6次数据，存储到一个map当中
            readEdgeSet(color0,color1);
            readEdgeSet(color1,color0);
            readEdgeSet(color0,color2);
            readEdgeSet(color2,color0);
            readEdgeSet(color1,color2);
            readEdgeSet(color2,color1);
        }

        public long triangleNum() throws IOException, URISyntaxException, InterruptedException {

            buildGraphPartition();


            for (Map.Entry<Integer,HashSet<Integer>> entry : map.entrySet()) {
                Integer source = entry.getKey();
                HashSet<Integer> sNeighbours = entry.getValue();
                for (Integer target : sNeighbours) {
                    if (map.containsKey(target)) {
                        HashSet<Integer> tNeighbours = map.get(target);
                        for (Integer w : tNeighbours) {
                            if (sNeighbours.contains(w)) {
//                            System.out.println("colors=" + color0 + "," + color1 + ","  + color2);
//                            System.out.println("triangle=<" + source + "," + target + "," + w + ">");
                                triangleCount += bitrianlgeNum(source,target,w);
                            }
                        }
                    }
                }
            }
            return triangleCount;
        }

        private long bitrianlgeNum(int i, int j, int k) {
            Tuple2<Integer,Integer> ij = new Tuple2<>(i,j);
            Tuple2<Integer,Integer> ik = new Tuple2<>(i,k);
            Tuple2<Integer,Integer> jk = new Tuple2<>(j,k);
            long result = 0L;
            if (edgeProjectedWeight.containsKey(ij) && edgeProjectedWeight.containsKey(jk) && edgeProjectedWeight.containsKey(ik)) {
                HashSet<Integer> array1 = edgeProjectedWeight.get(ij);
                HashSet<Integer> array2 = edgeProjectedWeight.get(ik);
                HashSet<Integer> array3 = edgeProjectedWeight.get(jk);

                for (Integer n1 : array1) {
                    for (Integer n2 : array2) {
                        for (Integer n3 : array3) {
                            if (n1 != n2 && n2 != n3 && n1 != n3) {
//                                System.out.println("<i" + n1 + ",j" + n3 + "," +"k" + n2 + ">");
                                result+=1;
                            }
                        }
                    }
                }

            }
            return result;
        }
    }

    private static class ProblemFor2Colors {

        private int numColors;

        private Integer color0;
        private Integer color1;

        private Map<Integer, HashSet<Integer>> map;
        private Map<Tuple2<Integer,Integer>,HashSet<Integer>> edgeProjectedWeight;
        private Long triangleCount;

        private FileSystem fs;

        private String dataPathSuffix;

        public ProblemFor2Colors(int numColors, Integer color1, Integer color2, FileSystem fs, String dataPathSuffix) {
            this.numColors = numColors;

            this.color0 = color1;
            this.color1 = color2;

            this.map = new HashMap<>();
            this.edgeProjectedWeight = new HashMap<>();
            this.triangleCount = 0L;
            this.fs = fs;
            this.dataPathSuffix = dataPathSuffix;
        }

        public void readEdgeSet (Integer uColor, Integer vColor) throws IOException, URISyntaxException, InterruptedException {
            //TODO 读取数据6次然后存入hashMap中,比如颜色012，读取01，10，02，20，12，21

            Path edgeSetPath = new Path(dataPathSuffix + "/edge-" + uColor +"-" + vColor );

            if (!fs.exists(edgeSetPath)) {
                System.out.println("no exsist edge path");
                return;
            }

            Input in = new Input(new SnappyInputStream(fs.open(edgeSetPath).getWrappedStream()));
            while (true) {
                int u = in.readInt(true);
                if (u == -1) {
                    break;
                }
                int v = in.readInt(true);

                int w = in.readInt(true);

                if (map.containsKey(u)) {
                    HashSet<Integer> neighbours = map.get(u);
                    neighbours.add(v);
                    map.put(u,neighbours);
                } else {
                    HashSet<Integer> neighbours = new HashSet<>();
                    neighbours.add(v);
                    map.put(u,neighbours);
                }

                Tuple2<Integer,Integer> edge = new Tuple2<>(u,v);

                if (edgeProjectedWeight.containsKey(edge)) {
                    HashSet<Integer> weights = edgeProjectedWeight.get(edge);
                    weights.add(w);
                    edgeProjectedWeight.put(edge,weights);
                } else {
                    HashSet<Integer> weights = new HashSet<>();
                    weights.add(w);
                    edgeProjectedWeight.put(edge,weights);
                }
            }
            in.close();
        }



        public void buildGraphPartition() throws IOException, URISyntaxException, InterruptedException {

            readEdgeSet(color0,color0);
            readEdgeSet(color1,color1);
            readEdgeSet(color0,color1);
            readEdgeSet(color1,color0);
        }

        public long triangleNum() throws IOException, URISyntaxException, InterruptedException {
            buildGraphPartition();

            for (Map.Entry<Integer,HashSet<Integer>> entry : map.entrySet()) {
                Integer source = entry.getKey();
                HashSet<Integer> sNeighbours = entry.getValue();
                for (Integer i : sNeighbours) {
                    if (map.containsKey(i)) {
                        HashSet<Integer> tNeighbours = map.get(i);
                        for (Integer j : tNeighbours) {
                            if (sNeighbours.contains(j)) {

                                int sMod = source % numColors;
                                int iMod = i % numColors;
                                int jMod = j % numColors;

//                                Short colors0 = colors.get(0);
//                                Short colors1 = colors.get(1);

                                if (sMod == iMod && sMod == jMod ) {
                                    if (color0 + 1 == color1 % numColors && sMod == color0) {
//                                      System.out.println("colors=" + color0 + "," + color0 + ",");
//                                      System.out.println("triangle=<" + source + "," + i + "," + j + ">");
                                        triangleCount += bitrianlgeNum(source, i, j);
                                    }

                                    if (color0 == 0 && color1 == numColors-1 && sMod==numColors-1) {
//                                      System.out.println("colors=" + color0 + "," + color0 + ",");
//                                      System.out.println("triangle=<" + source + "," + i + "," + j + ">");
                                        triangleCount += bitrianlgeNum(source, i, j);
                                    }
                                } else  {
//                                    System.out.println("colors=" + color0 + "," + color0 + ",");
//                                    System.out.println("triangle=<" + source + "," + i + "," + j + ">");
                                    triangleCount += bitrianlgeNum(source,i,j);
                                }
                            }
                        }
                    }
                }
            }
            return triangleCount;
        }


        private long bitrianlgeNum(int i, int j, int k) {
            Tuple2<Integer,Integer> ij = new Tuple2<>(i,j);
            Tuple2<Integer,Integer> ik = new Tuple2<>(i,k);
            Tuple2<Integer,Integer> jk = new Tuple2<>(j,k);
            long result = 0L;
            if (edgeProjectedWeight.containsKey(ij) && edgeProjectedWeight.containsKey(jk) && edgeProjectedWeight.containsKey(ik)) {
                HashSet<Integer> array1 = edgeProjectedWeight.get(ij);
                HashSet<Integer> array2 = edgeProjectedWeight.get(ik);
                HashSet<Integer> array3 = edgeProjectedWeight.get(jk);

                for (Integer n1 : array1) {
                    for (Integer n2 : array2) {
                        for (Integer n3 : array3) {
                            if (n1 != n2 && n2 != n3 && n1 != n3) {
//                                System.out.println("<i" + n1 + ",j" + n3 + "," +"k" + n2 + ">");
                                result+=1;
                            }
                        }
                    }
                }
            }
            return result;
        }
    }

    //构造A型和V型wedge
    private static class readEdgesForWedgeTypeA implements PairFlatMapFunction<Iterator<String>,Integer, Integer> {
        @Override
        public Iterator<Tuple2<Integer, Integer>> call(Iterator<String> in) throws Exception {
            ArrayList<Tuple2<Integer, Integer>> temp = new ArrayList<>();
            while (in.hasNext()) {
                String item = in.next();
                String[] fields = item.split(DELIMITER);
                Integer a = Integer.parseInt(fields[0]);
                Integer v = Integer.parseInt(fields[fields.length - 1]);
                temp.add(new Tuple2<>(a , v));
            }
            return temp.iterator();
        }
    }
    private static class readEdgesForWedgeTypeV implements PairFlatMapFunction<Iterator<String>,Integer, Integer> {
        @Override
        public Iterator<Tuple2<Integer, Integer>> call(Iterator<String> in) throws Exception {
            ArrayList<Tuple2<Integer, Integer>> temp = new ArrayList<>();
            while (in.hasNext()) {
                String item = in.next();
                String[] fields = item.split(DELIMITER);
                Integer a = Integer.parseInt(fields[0]);
                Integer v = Integer.parseInt(fields[fields.length - 1]);
                temp.add(new Tuple2<>(v , a));
            }
            return temp.iterator();
        }
    }

    private static class CreateCombinerMv implements Function<Integer, HashSet<Integer>> {

        @Override
        public HashSet<Integer> call(Integer firstValue) throws Exception {
            HashSet<Integer> mvNeighboursSet = new HashSet<>();
            mvNeighboursSet.add(firstValue);
            return mvNeighboursSet;
        }
    }
    private static class MergeValueMv implements Function2< HashSet<Integer>,Integer,  HashSet<Integer>> {
        @Override
        public HashSet<Integer> call(HashSet<Integer> mvPartition, Integer secondValue) throws Exception {
            mvPartition.add(secondValue);
            return mvPartition;
        }
    }
    private static class MergeCombinerMv implements Function2<HashSet<Integer>, HashSet<Integer>, HashSet<Integer>> {
        @Override
        public HashSet<Integer> call(HashSet<Integer> v1, HashSet<Integer> v2) throws Exception {

            HashSet<Integer> mvNeighboursSet1 = v1;
            HashSet<Integer> mvNeighboursSet2 = v2;

            if (mvNeighboursSet1.size() <= mvNeighboursSet2.size()) {

                for (Integer item : mvNeighboursSet1) {
                    mvNeighboursSet2.add(item);
                }
                return mvNeighboursSet2;

            }

            for (Integer item : mvNeighboursSet2) {
                mvNeighboursSet1.add(item);
            }
            return mvNeighboursSet1;

        }
    }

    //对wedgeV进行分类(拐点取模)，然后写入外存
    private static class wedgeVToHdfsPartitioner extends Partitioner {

        private int numPartition;

        public wedgeVToHdfsPartitioner(int numPartition) {
            this.numPartition = numPartition;
        }
        public int getNumPartition() {
            return numPartition;
        }

        @Override
        public int numPartitions() {
            return getNumPartition();
        }

        @Override
        public int getPartition(Object key) {

            Integer partitionId = ((Integer) key) % numPartition;

            return partitionId;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof wedgeVToHdfsPartitioner) {
                wedgeVToHdfsPartitioner partitionerObject = (wedgeVToHdfsPartitioner) obj;
                if (partitionerObject.getNumPartition() == this.getNumPartition())
                    return true;
            }
            return false;
        }
    }

    //对wedgeA进行分类(拐点取模),然后读取外存中的V型wedge构造二分图的边
    private static class wedgeAMiddleVetextPartitioner extends Partitioner {

        private int numPartition;

        public wedgeAMiddleVetextPartitioner(int numPartition) {

            this.numPartition = numPartition;
        }
        public int getNumPartition() {
            return numPartition;
        }

        @Override
        public int numPartitions() {
            return getNumPartition();
        }

        @Override
        public int getPartition(Object key) {

            Integer partitionId = ((Integer) key) % numPartition;

            return partitionId;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof wedgeAMiddleVetextPartitioner) {
                wedgeAMiddleVetextPartitioner partitionerObject = (wedgeAMiddleVetextPartitioner) obj;
                if (partitionerObject.getNumPartition() == this.getNumPartition())
                    return true;
            }
            return false;
        }
    }


    //用于edgeBlock分类
    private static class EdgeBlockToPairRDD implements PairFlatMapFunction<Iterator<Tuple3<Integer,Integer,Integer>>, Integer, Tuple2<Integer,Integer>> {
        @Override
        public Iterator<Tuple2<Integer, Tuple2<Integer,Integer>>> call(Iterator<Tuple3<Integer,Integer,Integer>> in) throws Exception {
            ArrayList<Tuple2<Integer, Tuple2<Integer,Integer>>> result = new ArrayList<>();
            while (in.hasNext()) {
                Tuple3<Integer,Integer, Integer> problem = in.next();

                Integer i = problem._1();
                Integer j = problem._2();
                Integer key = problem._3();
                result.add(new Tuple2<>(key,new Tuple2<>(i,j)));
            }
            return result.iterator();
        }
    }
    private static class EdgeBlockPartitioner extends Partitioner {

        private int numPartition;

        public int getNumPartition() {
            return numPartition;
        }

        public EdgeBlockPartitioner(int numPartition) {
            this.numPartition = numPartition;
        }

        @Override
        public int numPartitions() {
            return getNumPartition();
        }

        @Override
        public int getPartition(Object key) {

            int partitionId = (int) key;

            return partitionId;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof EdgeBlockPartitioner) {
                EdgeBlockPartitioner partitionerObject = (EdgeBlockPartitioner) obj;
                if (partitionerObject.getNumPartition() == this.getNumPartition())
                    return true;
            }
            return false;
        }
    }


}

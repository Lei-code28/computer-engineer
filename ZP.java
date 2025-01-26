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


public class ZP {

//    private static String inputHdfsPath = "/chapter4/input/";
//    private static String outputHdfsPath = "/chapter4/experiment/";

    private static final String DELIMITER = " ";

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("ZP" + "-"   +  args[6] +
                        "-coresMax-"     +  args[0] + "-exCores-"   + args[1] + "-exMemory-"    +  args[2]  +
                                "-memoryF-"  +  args[3] + "-memorySF-"  + args[4] + "_defaultPara_" +  args[5]  +
                                "-numA-"     +  args[7] + "-numV-"      + args[8] +
                                "-modeS-"    +  args[9] + "-modeE-"     + args[10])
                .set("spark.cores.max", args[0])
                .set("spark.executor.cores", args[1])
                .set("spark.executor.memory", args[2])

                .set("spark.memory.fraction",args[3])
                .set("spark.memory.storageFraction",args[4])

                .set("spark.default.parallelism", args[5])

                .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.registrationRequired","true")
                .set("spark.kryoserializer.buffer", "24m")
                .registerKryoClasses(new Class[]{Tuple2.class,ArrayList.class,HashSet.class,HashMap.class})
                ;

        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");

        String dataName = args[6];
        Integer partitionNumForTypeA = Integer.parseInt(args[7]);
        Integer partitionNumForTypeV = Integer.parseInt(args[8]);

        Integer modS = Integer.parseInt(args[9]);
        Integer modE = Integer.parseInt(args[10]);

//        String inputHdfsPath = "/chapter4/input/";
        String inputHdfsPath = "hdfs://10.176.24.42:9000/chapter4/input/";
        String outputHdfsPath = "/chapter4/experiment/ZP";

        String path1ForTypeAStorage = outputHdfsPath + "/ZP-" +dataName +"-numA"+ args[1] +"-numV"+args[2]+ "/" + "WedgeTypeA";
        String path2ForTypeVStorage = outputHdfsPath + "/ZP-" +dataName +"-numA"+ args[1] +"-numV"+args[2]+ "/" + "WedgeTypeV";
        String path3ForSuperWedge   = outputHdfsPath + "/ZP-" +dataName +"-numA"+ args[1] +"-numV"+args[2]+ "/" + "SuperWedge"  ;
        String path4ForResult       = outputHdfsPath + "/ZP-" +dataName +"-numA"+ args[1] +"-numV"+args[2]+ "/" + "Result"  ;

        //TODO Step1 创建存储edgeSet的hdfs文件夹和读取初始数据
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(new URI("hdfs://10.176.24.42:9000"), conf, "star");

            fs.mkdirs(new Path(path1ForTypeAStorage));
            fs.mkdirs(new Path(path2ForTypeVStorage));
        } catch (IOException | URISyntaxException | InterruptedException e) { e.printStackTrace(); }

        LongAccumulator totalCount = sc.sc().longAccumulator();

        JavaRDD<String> inputData = sc.textFile(inputHdfsPath + dataName);

        //TODO Step2 构造出A型wedge，并将其写入外存
        JavaPairRDD<Integer, HashSet<Integer>> wedgeTypeA = inputData.mapPartitionsToPair(new readEdgesForWedgeTypeA()).combineByKey(new CreateCombinerMv(), new MergeValueMv(), new MergeCombinerMv());
        JavaPairRDD<Integer, HashSet<Integer>> wedgeTypeAPartition = wedgeTypeA.partitionBy(new wedgeToHdfsPartitioner(partitionNumForTypeA));
        wedgeTypeAPartition.foreachPartition(it -> {
            if (it.hasNext()) {
                Configuration conf = new Configuration();
                FileSystem fs = FileSystem.get(new URI("hdfs://10.176.24.42:9000"), conf, "star");
                Path edgeSetPath = new Path(path1ForTypeAStorage + "/A-" + TaskContext.getPartitionId());
                Output output = new Output(new SnappyOutputStream(fs.create(edgeSetPath).getWrappedStream()));
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

        //TODO Step3 构造出V型wedge，并将其写入外存
        JavaPairRDD<Integer, HashSet<Integer>> wedgeTypeV = inputData.mapPartitionsToPair(new readEdgesForWedgeTypeV()).combineByKey(new CreateCombinerMv(), new MergeValueMv(), new MergeCombinerMv());
        wedgeTypeV.partitionBy(new wedgeToHdfsPartitioner(partitionNumForTypeV)).foreachPartition(it -> {
            if (it.hasNext()) {
                Configuration conf = new Configuration();
                FileSystem fs = FileSystem.get(new URI("hdfs://10.176.24.42:9000"), conf, "star");
                Path edgeSetPath = new Path(path2ForTypeVStorage + "/V-" + TaskContext.getPartitionId());
                Output output = new Output(new SnappyOutputStream(fs.create(edgeSetPath).getWrappedStream()));
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

        //TODO Step4 wedgeTypeApartition读取V型wedge数据构造Super-wedge
        wedgeTypeAPartition.foreachPartition(it -> {
            if (it.hasNext()) {

                Configuration conf = new Configuration();
                FileSystem fs = FileSystem.get(new URI("hdfs://10.176.24.42:9000"), conf, "star");

                String pathA = path1ForTypeAStorage + "/A-";
                String pathV = path2ForTypeVStorage + "/V-";

                HashMap<Integer,HashSet<Integer>> mapForPartitionTypeA = new HashMap<>();
                HashMap<Integer,HashSet<Integer>> mapForTypeV = new HashMap<>();

                HashMap<Integer,HashMap<Integer,HashSet<Integer>>> mapForGroupedTypeA = new HashMap<>();
                HashMap<Integer,HashMap<Integer,HashSet<Integer>>> mapForGroupedTypeV = new HashMap<>();

                HashSet<Integer> fNrAll = new HashSet<>(); //存储整个partition中A型wedge的一阶邻居，用于过滤。
                HashSet<Integer> fNrAllKeys = new HashSet<>();//存储所有A型wedge的一阶邻居的key
                HashSet<Integer> fNrDegree1 = new HashSet<>(); //存储整个partition中A型wedge度为1的一阶邻居。

                HashSet<Integer> sNrAll = new HashSet<>(); //存储所有V型wedge的一阶邻居，用于过滤。
                HashSet<Integer> sNrAllKeys = new HashSet<>();//存储所有V型wedge的一阶邻居的key
                HashSet<Integer> sNrDegree1 = new HashSet<>(); //存储整个partition中V型wedge度为1的一阶邻居。

                //TODO Step4.1 遍历整个partition中所有的A型Wedge，将它们的一阶邻居存入fNrAll，并取模存入fNrAllKey
                while(it.hasNext()) {
                    Tuple2<Integer, HashSet<Integer>> singleWedgeA = it.next();
                    Integer mV = singleWedgeA._1;
                    HashSet<Integer> fNrs = singleWedgeA._2;

                    HashMap<Integer, HashSet<Integer>> temp = new HashMap<>();
                    //rule1 过滤一阶邻居数量只有1个的wedge
                    if (fNrs.size() >= 2) {

                        mapForPartitionTypeA.put(mV,fNrs);
                        for (Integer n : fNrs) {
                            fNrAll.add(n);
                            int key = n % partitionNumForTypeV;//用于读取V型wedge的块，还需要一个modeS和modeE，
                            fNrAllKeys.add(key);

                            //对这个wedge的Fnr分组
                            int keyForEndPoint = n % modE;
                            if (temp.containsKey(keyForEndPoint)) {
                                HashSet<Integer> group = temp.get(keyForEndPoint);
                                group.add(n);
                                temp.put(keyForEndPoint,group);
                            } else {
                                HashSet<Integer> group = new HashSet<>();
                                group.add(n);
                                temp.put(keyForEndPoint,group);
                            }
                        }

                        mapForGroupedTypeA.put(mV,temp);
                    }
                }

                //TODO Step4.2 遍历fNrAllKey中所包含的块，读取所需要的V型wedge的块存入mapForTypeV 在读取时，需要根据fNrAll进行过滤，避免储存额外的V型
                for (Integer item : fNrAllKeys) {

                    Path tempPathForV = new Path(pathV + item);
                    if (fs.exists(tempPathForV)) {
                        Input in = new Input(new SnappyInputStream(fs.open(tempPathForV).getWrappedStream()));
                        while (true) {
                            int mV = in.readInt(true);
                            if (mV == -1) {
                                break;
                            }
                            int nSize = in.readInt(true);

                            if (!fNrAll.contains(mV)) { //当fNrAll不包含该拐点时，仍然读取但是不用存储，
                                for (int i = 1; i <= nSize; i++) {
                                    in.readInt(true);
                                }
                            } else if (fNrAll.contains(mV)) { //fNrAll包含该拐点时，进一步判断该拐点wedge一阶邻居的数量
                                if (nSize < 2) {
                                    in.readInt(true);
                                    fNrDegree1.add(mV);
                                } else {


                                    HashSet<Integer> neighbours = new HashSet<>();

                                    HashMap<Integer, HashSet<Integer>> temp = new HashMap<>();

                                    for (int i = 1; i <= nSize; i++) {
                                        int n = in.readInt(true);
                                        neighbours.add(n);

                                        //将V型的fNr即sNr存入sNrAll和sNrAllKey
                                        sNrAll.add(n);
                                        int sNrKey = n % partitionNumForTypeA;
                                        sNrAllKeys.add(sNrKey);

                                        //对读取的V型wedge的fNr(sNr)进行分组
                                        int keyForStartPoint = n % modS;
                                        if (temp.containsKey(keyForStartPoint)) {
                                            HashSet<Integer> group = temp.get(keyForStartPoint);
                                            group.add(n);
                                            temp.put(keyForStartPoint, group);
                                        } else {
                                            HashSet<Integer> group = new HashSet<>();
                                            group.add(n);
                                            temp.put(keyForStartPoint, group);
                                        }

                                    }
                                    //读取到的数据mV都是唯一的，因此不需要判断map.contains
                                    mapForTypeV.put(mV, neighbours);
                                    mapForGroupedTypeV.put(mV, temp);
                                }
                            }
                        }
                        in.close();
                    }
                }
//                fNrAll.clear();
//                fNrAllKeys.clear();

                //TODO Step4.3 遍历sNrAllKey中所包含的块，并不为读取A型wedge，而是将其中度为1的点写入sNrdegree1 在读取时，需要根据sNrAll进行过滤，避免储存额外的A型
                for (Integer item : sNrAllKeys) {
                    Path tempPathForA = new Path(pathA + item);
                    if (fs.exists(tempPathForA)) {
                        Input in = new Input(new SnappyInputStream(fs.open(tempPathForA).getWrappedStream()));
                        while (true) {
                            int mV = in.readInt(true);
                            if (mV == -1) {
                                break;
                            }
                            int nSize = in.readInt(true);

                            if (!sNrAll.contains(mV)) {
                                for (int i = 1; i <= nSize; i++) {
                                    in.readInt(true);
                                }
                            } else if (sNrAll.contains(mV)) {
                                if (nSize < 2) {
                                    in.readInt(true);
                                    sNrDegree1.add(mV);
                                } else {
                                    for (int i = 1; i <= nSize; i++) {
                                        in.readInt(true);
                                    }
                                }
                            }
                        }
                        in.close();
                    }
                }



                HashMap<Tuple2<Integer,Integer>,ArrayList<Tuple2<Integer,Integer>>> zedgeMap = new HashMap<>(); //用于存储zedge

//              TODO Step4.4 遍历整个partition中所有需要计算的A型wedge,构造Super wedge
                for (Map.Entry<Integer,HashSet<Integer>> entry : mapForPartitionTypeA.entrySet()) {
                    Integer mVetexToCompute = entry.getKey();  //zedge的第二拐点
                    HashSet<Integer> mVetexfNrSet = entry.getValue();

                    HashSet<Integer> fNrKeySetOfModeE = new HashSet<>();
                    HashSet<Integer> mVetexfNrSetUpdate = new HashSet<>();

                    HashMap<Integer,HashSet<Integer>> sNrKeyGroupMap = new HashMap<>(); //用于存储每个V型wededge的neighbour 对 modeS求key之后分组的情况 //这里之前放到scope外公用

                    //TODO Step4.4.1 第一步遍历第二拐点的fNr，将其中度大于1的%modeE求key，存入fNrKeySet
                    for (Integer fNr : mVetexfNrSet) {
                        if (!fNrDegree1.contains(fNr)){

                            mVetexfNrSetUpdate.add(fNr);

                            int fNrKey = fNr % modE;
                            fNrKeySetOfModeE.add(fNrKey);


                                if (!sNrKeyGroupMap.containsKey(fNr)) {
                                    HashSet<Integer> sHopNrSet = mapForTypeV.get(fNr);


                                    HashSet<Integer> sHopNrSetUpdate = new HashSet<>();
                                    HashSet<Integer> sNrKeySetOfModeS = new HashSet<>();
                                    for (Integer sNr : sHopNrSet) {
                                        if (!sNrDegree1.contains(sNr)) {
                                            sHopNrSetUpdate.add(sNr);     //这里之前写成了fNr
                                            if (sNr < mVetexToCompute) { //sNr 为zwedge中的起点，id < 第一拐点
                                                int sNrKey = sNr % modS;
                                                sNrKeySetOfModeS.add(sNrKey);
                                            }
                                        }
                                    }
                                    mapForTypeV.put(fNr,sHopNrSetUpdate); //删除mapV中度为1的点，方便下一个wedge复用
                                    sNrKeyGroupMap.put(fNr,sNrKeySetOfModeS);
                                }
//                            }
                        }
                    }

                    //TODO Step4.4.2 第二步再次遍历第二拐点的fNr，对于度大于1的，访问以它为拐点的V型wedge
                    for (Integer fNr : mVetexfNrSetUpdate) { //fNr是zwedfge的第一拐点 //这里之前写成 mVetexfNrSet

                                    HashSet<Integer> sNrKeySet = sNrKeyGroupMap.get(fNr);
                                    //对应的key是由前面两个集合相乘决定
                                    for (Integer starPointKey : sNrKeySet) {
                                        for (Integer endPointKey : fNrKeySetOfModeE) {
                                            Tuple2<Integer, Integer> key = new Tuple2<>(starPointKey, endPointKey);
                                            if (zedgeMap.containsKey(key)) {
                                                ArrayList<Tuple2<Integer, Integer>> sw = zedgeMap.get(key);
                                                sw.add(new Tuple2<>(fNr, mVetexToCompute));
                                            } else {
                                                ArrayList<Tuple2<Integer, Integer>> sw = new ArrayList<>();
                                                sw.add(new Tuple2<>(fNr, mVetexToCompute));
                                                zedgeMap.put(key, sw);
                                            }
                                        }
                                    }

                    }
                }


                //TODO Step4.5 将整个zedgemap写入hdfs
                for (Map.Entry<Tuple2<Integer,Integer>,ArrayList<Tuple2<Integer,Integer>>> entry : zedgeMap.entrySet()) {
                    Tuple2<Integer, Integer> key = entry.getKey();
                    Integer startPointKey = key._1;
                    Integer endPointKey = key._2;
                    ArrayList<Tuple2<Integer, Integer>> Zedges = entry.getValue();

                    Path edgeSetPath = new Path(path3ForSuperWedge + "/" + startPointKey + "-" + endPointKey + "/" + TaskContext.getPartitionId());
                    Output output = new Output(new SnappyOutputStream(fs.create(edgeSetPath).getWrappedStream()));
//                    System.out.println("startPointKey=" + startPointKey + "," +  "endPointKey" + endPointKey);
                    for (Tuple2<Integer,Integer> sw : Zedges) {
                        Integer firstMv = sw._1;
                        Integer secondMv = sw._2;

                        output.writeInt(firstMv, true);

//                        System.out.print("startPoint set =");

                        HashMap<Integer, HashSet<Integer>> tempV = mapForGroupedTypeV.get(firstMv);
                        HashSet<Integer> startPoints = tempV.get(startPointKey);
                        for (Integer item : startPoints) {
                            if (!sNrDegree1.contains(item) && item < secondMv) { //这里之前缺少item < secondMv，printf 出来进行的修改
//                                System.out.print(item + ",");
                                output.writeInt(item, true);
                            }
                        }
                        output.writeInt(-1, true);


                        output.writeInt(secondMv, true);

                        HashMap<Integer, HashSet<Integer>> tempA = mapForGroupedTypeA.get(secondMv);
                        HashSet<Integer> endPoints = tempA.get(endPointKey);
                        for (Integer item : endPoints) {
                            if (!fNrDegree1.contains(item) && item != firstMv ) {
//                                System.out.print(item + ",");
                                output.writeInt(item, true);
                            }
                        }
                        output.writeInt(-2, true);

//                        System.out.println("    ?? -2 ??    ");
                    }
                    output.writeInt(-3,true);
//                    System.out.println("xxxxxxx -3");
                    output.close();
                }

            }
        });

        //TODO Step5 构造组合数，访问每一个文件夹中的数据
        int zedgeBlockKey = 0;
        ArrayList<Tuple3<Integer,Integer,Integer>> zedgeBlocks= new ArrayList<>();

        for(int i = 0; i <= modS -1; i++) {
            for (int j = 0; j <= modE -1; j++) {
                zedgeBlocks.add(new Tuple3<>(i, j, zedgeBlockKey++));
            }
        }

//        System.out.println("zedge Blocks" + zedgeBlocks);

        JavaPairRDD<Integer, Tuple2<Integer, Integer>> zedgeBlockPartition = sc.parallelize(zedgeBlocks).mapPartitionsToPair(new SuperWedgeBlockToPairRDD());

        JavaPairRDD<Tuple4<Integer, Integer, Integer, Integer>, Byte> index = zedgeBlockPartition.mapPartitionsToPair(new mapKeysToIndexForSuperWedge(partitionNumForTypeA));
//        index.foreach(it -> System.out.println("index" + it));
        JavaPairRDD<Tuple4<Integer, Integer, Integer, Integer>, Byte> partitionToCompute = index.partitionBy(new SuperWedgeBlockPartitioner(modS, modE, partitionNumForTypeA));

        partitionToCompute.foreachPartition(it -> {

            while (it.hasNext()) {
                long count = 0l;
                Tuple2<Tuple4<Integer, Integer, Integer, Integer>, Byte> temp = it.next();
                Tuple4<Integer, Integer, Integer, Integer> zedeKeys = temp._1;

                Integer key1_startPointKey = zedeKeys._1();
                Integer key2_endPointKey = zedeKeys._2();
                Integer key3_partitionKey1ForWedgeTypeA = zedeKeys._3();
                Integer key4_partitionKey2ForWedgeTypeA = zedeKeys._4();

                Configuration conf = new Configuration();
                FileSystem fs = FileSystem.get(new URI("hdfs://10.176.24.42:9000"), conf, "star");

                if (key3_partitionKey1ForWedgeTypeA == key4_partitionKey2ForWedgeTypeA) {

                    String pathStringForSw1 = path3ForSuperWedge + "/" +  key1_startPointKey + "-" + key2_endPointKey + "/"+ key3_partitionKey1ForWedgeTypeA;
                    Path path1 = new Path(pathStringForSw1);
//                    System.out.println("key1=" + key1_startPointKey + ",key2=" + key2_endPointKey + ",key3=" + key3_partitionKey1ForWedgeTypeA + ",key4=" + key4_partitionKey2ForWedgeTypeA + "--------------k3 equal to k4");

                    if (fs.exists(path1)) {

                        Map<Tuple2<Integer,Integer>,ArrayList<Tuple2<Integer,Integer>>> zedgeMap = new HashMap<>();

                        //读取数据
                        Input in1 = new Input(new SnappyInputStream(fs.open(new Path(pathStringForSw1)).getWrappedStream()));
                        while (true) {
                            int fisrtMv = in1.readInt(true);
                            if (fisrtMv == -3) break;
//                            System.out.print("firstMv=" + fisrtMv + "set1=");
                            HashSet<Integer> startPointSet = new HashSet<>();
                            while (true) {
                                int startPoint = in1.readInt(true);
                                if (startPoint == -1) break;
                                startPointSet.add(startPoint);
//                                System.out.print("," + startPoint);
                            }
                            int secondMv = in1.readInt(true);
//                            System.out.print(",secondMV=" + secondMv + "set2=");

                            HashSet<Integer> endPointSet = new HashSet<>();
                            while (true) {
                                int endPoint = in1.readInt(true);
                                if (endPoint == -2) break;
//                                System.out.println("," + endPoint);

                                for (Integer startPoint : startPointSet ) {
                                    Tuple2<Integer,Integer> key = new Tuple2<>(startPoint,endPoint);
                                    if (!zedgeMap.containsKey(key)) {

                                        ArrayList<Tuple2<Integer,Integer>> arrayList = new ArrayList<>();

                                        Tuple2<Integer, Integer> value = new Tuple2<>(fisrtMv, secondMv);

                                        arrayList.add(value);

                                        zedgeMap.put(key,arrayList);
                                    } else {
                                        ArrayList<Tuple2<Integer, Integer>> arrayList = zedgeMap.get(key);
                                        for (Tuple2<Integer,Integer> item : arrayList) {
                                            Integer f = item._1;
                                            Integer s = item._2;
                                            if (f != fisrtMv && s != secondMv) {
//                                                System.out.print(startPoint + "," +  s + "," + secondMv);
//                                                System.out.println("," + f + "," + fisrtMv + "," + endPoint);
                                                count++;
                                            }
                                        }
                                        Tuple2<Integer, Integer> value = new Tuple2<>(fisrtMv, secondMv);
                                        arrayList.add(value);
                                        zedgeMap.put(key,arrayList);
                                    }
                                }


//                                endPointSet.add(endPoint);

                            }
//                            System.out.println("");
                        }
                        in1.close();

//                        System.out.println("mv to compute" + mVToCompute);
//                        System.out.println("edge map =" + edgeMap);
//                        System.out.println("zedge map=" + zedgeMap);

                    }

                }
                else {

//                    System.out.println("key1=" + key1_startPointKey + ",key2=" + key2_endPointKey + ",key3=" + key3_partitionKey1ForWedgeTypeA + ",key4=" + key4_partitionKey2ForWedgeTypeA + "--------------k3 not equal to k4");

                    String pathStringForSw1 = path3ForSuperWedge + "/" +  key1_startPointKey + "-" + key2_endPointKey + "/"+ key3_partitionKey1ForWedgeTypeA;
                    Path path1 = new Path(pathStringForSw1);

                    String pathStringForSw2 = path3ForSuperWedge + "/" +  key1_startPointKey + "-" + key2_endPointKey + "/" +key4_partitionKey2ForWedgeTypeA;
                    Path path2 = new Path(pathStringForSw2);


                    if (fs.exists(path1) && fs.exists(path2)){

                        Map<Tuple2<Integer,Integer>,ArrayList<Tuple2<Integer,Integer>>> zedgeMap = new HashMap<>();

                        Input in1 = new Input(new SnappyInputStream(fs.open(new Path(pathStringForSw1)).getWrappedStream()));
                        while (true) {
                            int fisrtMv = in1.readInt(true);
                            if (fisrtMv == -3) break;
//                            System.out.print("firstMv=" + fisrtMv + "set1=");
                            HashSet<Integer> startPointSet = new HashSet<>();
                            while (true) {
                                int startPoint = in1.readInt(true);
                                if (startPoint == -1) break;
                                startPointSet.add(startPoint);
//                                System.out.print("," + startPoint);
                            }
                            int secondMv = in1.readInt(true);
//                            System.out.print(",secondMV=" + secondMv + "set2=");

//                            HashSet<Integer> endPointSet = new HashSet<>();
                            while (true) {
                                int endPoint = in1.readInt(true);
                                if (endPoint == -2) break;
//                                System.out.print("," + endPoint);

                                for (Integer startPoint : startPointSet ) {
                                    Tuple2<Integer,Integer> key = new Tuple2<>(startPoint,endPoint);
                                    if (!zedgeMap.containsKey(key)) {

                                        ArrayList<Tuple2<Integer,Integer>> arrayList = new ArrayList<>();
                                        Tuple2<Integer, Integer> value = new Tuple2<>(fisrtMv, secondMv);

                                        arrayList.add(value);
                                        zedgeMap.put(key,arrayList);
                                    } else {
                                        ArrayList<Tuple2<Integer, Integer>> arrayList = zedgeMap.get(key);
                                        Tuple2<Integer, Integer> value = new Tuple2<>(fisrtMv, secondMv);
                                        arrayList.add(value);
                                        zedgeMap.put(key,arrayList);
                                    }
                                }

//                                endPointSet.add(endPoint);

                            }
//                            System.out.println("");

                        }
                        in1.close();

                        Input in2 = new Input(new SnappyInputStream(fs.open(new Path(pathStringForSw2)).getWrappedStream()));
                        while (true) {
                                int fisrtMv = in2.readInt(true);
                                if (fisrtMv == -3) break;
//                                System.out.print("firstMv=" + fisrtMv + "set1=");
                                HashSet<Integer> startPointSet = new HashSet<>();
                                while (true) {
                                    int startPoint = in2.readInt(true);
                                    if (startPoint == -1) break;
                                    startPointSet.add(startPoint);
//                                    System.out.print("," + startPoint);
                                }

                                int secondMv = in2.readInt(true);
//                                System.out.print(",secondMV=" + secondMv + "set2=");
//                                HashSet<Integer> endPointSet = new HashSet<>();
                                while (true) {
                                    int endPoint = in2.readInt(true);
                                    if (endPoint == -2) break;


//                                    System.out.print("," + endPoint);
                                    for (Integer startPoint : startPointSet ) {
                                        Tuple2<Integer,Integer> key = new Tuple2<>(startPoint,endPoint);
                                        if (!zedgeMap.containsKey(key)) {

                                            ArrayList<Tuple2<Integer,Integer>> arrayList = new ArrayList<>();

                                            Tuple2<Integer, Integer> value = new Tuple2<>(fisrtMv, secondMv);

                                            arrayList.add(value);

                                            zedgeMap.put(key,arrayList);
                                        } else {
                                            ArrayList<Tuple2<Integer, Integer>> arrayList = zedgeMap.get(key);
                                            for (Tuple2<Integer,Integer> item : arrayList) {
                                                Integer f = item._1;
                                                Integer s = item._2;
                                                if (f != fisrtMv && s != secondMv) {

//                                                    System.out.print(startPoint + "-" +  s + "-" + secondMv);
//                                                    System.out.println("-" + f + "-" + fisrtMv + "-" + endPoint);

                                                    count++;
                                                }
                                            }
                                            Tuple2<Integer, Integer> value = new Tuple2<>(fisrtMv, secondMv);
                                            arrayList.add(value);
                                            zedgeMap.put(key,arrayList);
                                        }
                                    }

//                                    endPointSet.add(endPoint);

                                }
//                                System.out.println("");


                        }
                        in2.close();

//                        System.out.println("mVToCompute1" + mVToCompute1 + "," + "mVToCompute2" + mVToCompute2);
//                        System.out.println("edgeMap" + edgeMap);
//                        System.out.println("zedge map" + zedgeMap);



                    }
                }

//                System.out.println("xxxxxxxxxxxxxxxxxxxxxxx partition split line");
//                System.out.println("");
//                System.out.println("");

                if (count != 0) {
                    totalCount.add(count);
//                    String path4 = path4ForResult + "/" + "partitionId-" + TaskContext.getPartitionId() + "-" + "key =" + zedeKeys  + "-" + count ;
//                    fs.mkdirs(new Path(path4));
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
    //构造wedge组
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
    //wedge写入外存
    private static class wedgeToHdfsPartitioner extends Partitioner {

        private int numPartition;

        public int getNumPartition() {
            return numPartition;
        }

        public wedgeToHdfsPartitioner(int numPartition) {
            this.numPartition = numPartition;
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
            if (obj instanceof wedgeToHdfsPartitioner) {
                wedgeToHdfsPartitioner partitionerObject = (wedgeToHdfsPartitioner) obj;
                if (partitionerObject.getNumPartition() == this.getNumPartition())
                    return true;
            }
            return false;
        }
    }


    private static class SuperWedgeBlockToPairRDD implements PairFlatMapFunction<Iterator<Tuple3<Integer,Integer,Integer>>, Integer, Tuple2<Integer,Integer>> {
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

    private static class mapKeysToIndexForSuperWedge implements PairFlatMapFunction<Iterator<Tuple2<Integer, Tuple2<Integer, Integer>>>, Tuple4<Integer,Integer,Integer,Integer>, Byte> {

        private Integer swPartitionNumWithSameKey;

        public mapKeysToIndexForSuperWedge(Integer partitionNumForTypeA) {
            this.swPartitionNumWithSameKey = partitionNumForTypeA;
        }

        @Override
        public Iterator<Tuple2<Tuple4<Integer, Integer,Integer,Integer>, Byte>> call(Iterator<Tuple2<Integer, Tuple2<Integer, Integer>>> in) throws Exception {
            ArrayList<Tuple2<Tuple4<Integer,Integer,Integer,Integer>,Byte>> result = new ArrayList<>();
            while (in.hasNext()) {
                Tuple2<Integer, Tuple2<Integer, Integer>> next = in.next();
                Tuple2<Integer, Integer> keys = next._2;
                Integer startPointKey = keys._1;
                Integer endPointKey = keys._2;

                for (int i = 0; i <= swPartitionNumWithSameKey-1; i++) {
                    Tuple4<Integer, Integer, Integer, Integer> temp = new Tuple4<>(startPointKey, endPointKey, i, i);
                    result.add(new Tuple2<>(temp, (byte) 0)); //这里0在后续不会使用到，单纯是占位作用。
                }

                for (int i = 0; i <= swPartitionNumWithSameKey-2; i++) {
                    for (int j = i+1; j <= swPartitionNumWithSameKey-1; j++) {
                            Tuple4<Integer, Integer, Integer, Integer> temp = new Tuple4<>(startPointKey, endPointKey, i, j);
                            result.add(new Tuple2<>(temp, (byte) 0)); //这里0在后续不会使用到，单纯是占位作用。
                    }
                }


            }
            return result.iterator();
        }
    }
    //对组合数分区，注意分区数量已经分区id
    private static class SuperWedgeBlockPartitioner extends Partitioner {

        private int modeS;

        private int modeE;

        private int partitionNumForTypeA;

        public SuperWedgeBlockPartitioner(int modeS, int modeE, int partitionNumForTypeA) {
            this.modeS = modeS;
            this.modeE = modeE;
            this.partitionNumForTypeA = partitionNumForTypeA;
        }

        @Override
        public int numPartitions() {
            return getNumPartition();
        }

        public int getNumPartition() {
            return modeS * modeE * partitionNumForTypeA * partitionNumForTypeA;
        }

        @Override
        public int getPartition(Object key) {

            Tuple4<Integer,Integer,Integer,Integer> temp = (Tuple4<Integer,Integer,Integer,Integer>) key;

            Integer key1 = temp._1();
            Integer key2 = temp._2();
            Integer key3 = temp._3();
            Integer key4 = temp._4();

            //key1 = startPointKey
            //key2 = endPointKey
            //key3 = partitionNumKey1
            //key4 = partitionNumKey2

            //TODO 唯一确定一个key

            int partitionId = (key1 * modeE + key2) * (partitionNumForTypeA * partitionNumForTypeA) + (key3 * partitionNumForTypeA + key4);


            return partitionId;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof SuperWedgeBlockPartitioner) {
                SuperWedgeBlockPartitioner partitionerObject = (SuperWedgeBlockPartitioner) obj;
                if (partitionerObject.getNumPartition() == this.getNumPartition())
                    return true;
            }
            return false;
        }
    }


}

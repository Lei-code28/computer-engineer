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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;


public class WP {

//    private static String inputHdfsPath = "/chapter4/input/";
//    private static String outputHdfsPath = "/chapter4/experiment/";

    private static final String DELIMITER = " ";

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
             .setAppName("WP" + "-"   +  args[6] +
                                "-coresMax-"     +  args[0] + "-exCores-"  + args[1] + "-exMemory-"    +  args[2]  +
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
                .registerKryoClasses(new Class[]{Tuple2.class,ArrayList.class,HashSet.class,HashMap.class})
                ;

        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");

        String dataName = args[6];
        Integer partitionNumForTypeA = Integer.parseInt(args[7]);
        Integer partitionNumForTypeV = Integer.parseInt(args[8]);

        Integer modS = Integer.parseInt(args[9]);
        Integer modE = Integer.parseInt(args[10]);


        String inputHdfsPath = "hdfs://10.176.24.42:9000/chapter4/input/";
        String outputHdfsPath = "/chapter4/experiment/WP";

        String path1ForTypeAStorage = outputHdfsPath + "/WP" +dataName +"-numA"+ args[7] +"-numV"+args[8]+"defalutP"+args[5] + "/" + "WedgeTypeA";
        String path2ForTypeVStorage = outputHdfsPath + "/WP" +dataName +"-numA"+ args[7] +"-numV"+args[8]+"defalutP"+args[5] + "/" + "WedgeTypeV";
        String path3ForResult       = outputHdfsPath + "/WP" +dataName +"-numA"+ args[7] +"-numV"+args[8]+"defalutP"+args[5] + "/" + "Result"  ;


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

        //TODO Step4 wedgeTypeApartition读取V型wedge数据计算
        wedgeTypeAPartition.foreachPartition(it -> {
            if (it.hasNext()) {
                Long biCount = 0L;
                Configuration conf = new Configuration();
                FileSystem fs = FileSystem.get(new URI("hdfs://10.176.24.42:9000"), conf, "star");

                String pathA = path1ForTypeAStorage + "/A-";
                String pathV = path2ForTypeVStorage + "/V-";

                HashMap<Integer,HashSet<Integer>> mapForPartitionTypeA = new HashMap<>();

                HashMap<Integer,HashSet<Integer>> mapForTypeA = new HashMap<>();
                HashMap<Integer,HashSet<Integer>> mapForTypeV = new HashMap<>();

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


                    if (fNrs.size() >= 2) {
                        mapForPartitionTypeA.put(mV,fNrs);
                        for (Integer n : fNrs) {
                            fNrAll.add(n);
                            int key = n % partitionNumForTypeV;
                            fNrAllKeys.add(key);

                        }
                    }

                }

                //TODO Step4.2 遍历fNrAllKey中所包含的块，读取所需要的V型wedge的块存入mapForTypeV
                //在读取时，需要根据fNrAll进行过滤，避免储存额外的V型
                for (Integer item : fNrAllKeys) {

                    Path temp = new Path(pathV + item);
                    if (fs.exists(temp)) {
                        Input in = new Input(new SnappyInputStream(fs.open(temp).getWrappedStream()));
                        while (true) {
                            int mV = in.readInt(true);
                            if (mV == -1) {
                                break;
                            }
                            int nSize = in.readInt(true);

                            if (!fNrAll.contains(mV)) { //当fNrAll不包含该拐点时，仍然读取但是不用存储
                                for (int i = 1; i <= nSize; i++) {
                                    in.readInt(true);
                                }

                            } else if (fNrAll.contains(mV)) { //fNrAll包含该拐点时，进一步判断该拐点wedge一阶邻居的数量
                                if (nSize < 2) {
                                    fNrDegree1.add(mV);
                                    in.readInt(true);
                                } else {
                                    HashSet<Integer> neighbours = new HashSet<>();
                                    for (int i = 1; i <= nSize; i++) {
                                        int n = in.readInt(true);
                                        neighbours.add(n);
                                        sNrAll.add(n);
                                        int sNrKey = n % partitionNumForTypeA;
                                        sNrAllKeys.add(sNrKey);
                                    }
                                    //读取到的数据mV都是唯一的，因此不需要判断map.contains
                                    mapForTypeV.put(mV, neighbours);
                                }
                            }
                        }
                        in.close();
                    }
                }
                fNrAll.clear();
                fNrAllKeys.clear();

                //TODO Step4.3 遍历sNrAllKey中所包含的块，读取所需要的A型wedge存入mapForType //在读取时，需要根据fNrAll进行过滤，避免储存额外的A型
                for (Integer item : sNrAllKeys) {

                    Path temp = new Path(pathA + item);
                    if (fs.exists(temp)) {
                        Input in = new Input(new SnappyInputStream(fs.open(temp).getWrappedStream()));
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
                                    sNrDegree1.add(mV);
                                    in.readInt(true);
                                } else {
                                    HashSet<Integer> neighbours = new HashSet<>();
                                    for (int i = 1; i <= nSize; i++) {
                                        int n = in.readInt(true);
                                        neighbours.add(n);
                                    }
                                    mapForTypeA.put(mV, neighbours);
                                }
                            }
                        }
                        in.close();
                    }
                }
                sNrAll.clear();
                sNrAllKeys.clear();

//                TODO Step4.4 遍历整个partition中所有需要计算的A型wedge
//                 存储各个wedge的交集

//                HashMap<Tuple2<Integer,Integer>,HashSet<Integer>> sharedData = new HashMap<>();

                for (Map.Entry<Integer,HashSet<Integer>> entry : mapForPartitionTypeA.entrySet()) {

                    Integer mVetexToCompute = entry.getKey();
                    HashSet<Integer> mVetexfNrSet = entry.getValue();

//                    HashSet<Integer> fNrToCompute = new HashSet<>();
                    HashMap<Integer,ArrayList<Integer>> sHopNrMap = new HashMap<>();
                    ArrayList<Integer> sHopNrList = new ArrayList<>(); //同于存储二阶邻居

                    //TODO Step4.4.1 搜集A型wedge拐点mVetex的二阶邻居存储到sHopNrList
                    for (Integer fNr : mVetexfNrSet) {
                        if (!fNrDegree1.contains(fNr)){
//                            fNrToCompute.add(fNr);
                            if (mapForTypeV.containsKey(fNr)) {
                                HashSet<Integer> sHopNrSet = mapForTypeV.get(fNr);
                                for (Integer sNr : sHopNrSet) {
                                    if (!sNrDegree1.contains(sNr) && sNr > mVetexToCompute) {
                                        if (sHopNrMap.containsKey(sNr)) {
                                            ArrayList<Integer> tempResult = sHopNrMap.get(sNr);
                                            tempResult.add(fNr);
                                            sHopNrMap.put(sNr, tempResult);
                                        } else {
                                            ArrayList<Integer> tempResult = new ArrayList<>();
                                            tempResult.add(fNr);
                                            sHopNrMap.put(sNr, tempResult);
                                            sHopNrList.add(sNr);
                                        }
                                    }
                                }
                            }
                        }
                    }


                    //TODO Step4.4.2 mVetex的每一对二阶邻居
                    //获得每一对二阶邻居的序号
                    for (int i = 0; i <= sHopNrList.size()-2; i++) {
                        for (int j = i + 1; j <= sHopNrList.size()-1; j++) {

                            Integer sHopNr1 = sHopNrList.get(i);
                            Integer sHopNr2 = sHopNrList.get(j);

                            HashSet<Integer> sHopNr1FNrSet = mapForTypeA.get(sHopNr1);
                            HashSet<Integer> sHopNr2FNrSet = mapForTypeA.get(sHopNr2);
                            HashSet<Integer> sHopNr1And2C = new HashSet<>();

                            int key1 = Math.min(sHopNr1,sHopNr2);
                            int key2 = Math.max(sHopNr1,sHopNr2);

                            Tuple2<Integer,Integer> key = new Tuple2<>(key1,key2);

//                            if (sharedData.containsKey(key)) {
//                                sHopNr1And2C = sharedData.get(key);
//                            } else  {

                                if (sHopNr1FNrSet.size() < sHopNr2FNrSet.size()) {
                                    for (Integer item : sHopNr1FNrSet) {
                                        if (sHopNr2FNrSet.contains(item)) {
                                            sHopNr1And2C.add(item);

                                        }
                                    }
                                } else {
                                    for (Integer item : sHopNr2FNrSet) {
                                        if (sHopNr1FNrSet.contains(item)) {
                                            sHopNr1And2C.add(item);

                                        }
                                    }
                                }
//                                sharedData.put(key,sHopNr1And2C);
//                            }


                            ArrayList<Integer> arrayList1 = sHopNrMap.get(sHopNr1);
                            ArrayList<Integer> arrayList2 = sHopNrMap.get(sHopNr2);

                            for (Integer item1 : arrayList1) {
                                for (Integer item2 : arrayList2) {
                                    for (Integer item3 : sHopNr1And2C) {
                                        if (item1 != item2 && item1 != item3 && item2 != item3) {
                                            biCount+=1;
                                        }
                                    }
                                }
                            }

                        }
                    }
                }

                if (biCount != 0) {
                    totalCount.add(biCount);
//                    String path4 = path3ForResult + "/" + "partitionId-" + TaskContext.getPartitionId() + "-" + biCount;
//                    fs.mkdirs(new Path(path4));
                }
            }
        });


        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(new URI("hdfs://10.176.24.42:9000"), conf, "star");

            String path4 = path3ForResult + "/total_bi-triangle_num =" + totalCount.value();
            fs.mkdirs(new Path(path4));

        } catch (IOException | URISyntaxException | InterruptedException e) { e.printStackTrace(); }

        sc.stop();
    }
    //构造wedge
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

}

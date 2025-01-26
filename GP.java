import com.esotericsoftware.kryo.io.Input;
import mpi.MPI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.shaded.org.jline.utils.InputStreamReader;
import org.xerial.snappy.SnappyInputStream;
import scala.Tuple2;
import scala.Tuple7;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

public class GP {
    public static void main(String[] args) {
        MPI.Init(args);

        int rank = MPI.COMM_WORLD.Rank();
        int size = MPI.COMM_WORLD.Size();

        int p = 100;

        if (rank == 0) {

            String inputHdfsPath = "hdfs://10.176.24.42:9000/chapter4/input/" + args[0];
            List<Tuple2<Integer,Integer>> edges = loadGraph(inputHdfsPath); // 生成测试边集 (a, v)

            Map<Integer,List<Tuple2<Integer,Integer>>> edgePartitions = partitionEdgesByHash(edges, p);

            List<Tuple2<Integer,Integer>> combinations = generateCombinations(p, edgePartitions);

            int totalCombinations = combinations.size();
            int chunkSize = totalCombinations / size;
            int remainder = totalCombinations % size;

            List<Tuple2<Integer,Integer>> mainChunk = combinations.subList(0, chunkSize + (remainder > 0 ? 1 : 0));
            processCombinations(mainChunk, rank);

            for (int i = 1; i < size; i++) {
                int start = i * chunkSize + Math.min(i, remainder);
                int end = (i + 1) * chunkSize + Math.min(i + 1, remainder);
                List<Tuple2<Integer,Integer>> chunk = combinations.subList(start, end);


                sendChunk(chunk, i);
            }

        } else {

            List<Tuple2<Integer,Integer>> chunk = receiveChunk(rank);
            processCombinations(chunk, rank);
        }

        MPI.Finalize();
    }


    public static List<Tuple2<Integer, Integer>> loadGraph(String hdfsPath) {
        List<Tuple2<Integer, Integer>> edges = new ArrayList<>();
        Configuration conf = new Configuration();

        try ( FileSystem fs = FileSystem.get(new URI("hdfs://10.176.24.42:9000"), conf, "star");
             BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(hdfsPath))))) {

            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(" "); // 假设边的格式为 "a v" （空格分隔）
                int a = Integer.parseInt(parts[0]);
                int v = Integer.parseInt(parts[1]);
                edges.add(new Tuple2<>(a, v));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return edges;
    }

    public static Map<Integer,List<Tuple2<Integer,Integer>>> partitionEdgesByHash(List<Tuple2<Integer,Integer>> edges, int p) {


        Map<Integer,List<Tuple2<Integer,Integer>>> par = new HashMap<>();
        for (int i = 0; i < p; i++) {
            par.put(i,new ArrayList<>());
        }

        // 按 (a, v) 的 a 值哈希取模进行划分
        for (Tuple2<Integer,Integer> edge : edges) {
            int a = edge._1;
            int hash = a % p; // 根据 a 值计算子集索引

            List<Tuple2<Integer, Integer>> temp = par.get(hash);
            temp.add(edge);
            par.put(hash,temp);

        }

        return par;
    }


    public static List<Tuple2<Integer,Integer>> generateCombinations(int combinationNum, Map<Integer,List<Tuple2<Integer,Integer>>> par
    ) {
        List<Tuple2<Integer,Integer>> combinations = new ArrayList<>();

        for(int i = 0; i <= combinationNum -6; i++) {
            for (int j = i + 1; j <= combinationNum - 5; j++) {
                for (int k = j + 1; k <= combinationNum - 4; k++) {
                    for (int l = k + 1; l <= combinationNum - 3; l++) {
                        for (int m = l + 1; m <= combinationNum - 2; m++) {
                            for (int n = m + 1; n <= combinationNum - 1; n++) {
                                combinations.addAll(par.get(i));
                                combinations.addAll(par.get(j));
                                combinations.addAll(par.get(k));
                                combinations.addAll(par.get(l));
                                combinations.addAll(par.get(m));
                                combinations.addAll(par.get(n));
                            }
                        }
                    }
                }
            }
        }




        return combinations;
    }


    public static void processCombinations(List<Tuple2<Integer,Integer>> edgePartitions, int rank) {

        HashMap<Integer,HashSet<Integer>> subgraphCombinatin = new HashMap<>();

        for (Tuple2<Integer,Integer> edge : edgePartitions) {
            Integer a = edge._1;
            Integer v = edge._2;

            if (subgraphCombinatin.containsKey(a)) {
                HashSet<Integer> temp = subgraphCombinatin.get(a);
                temp.add(v);
                subgraphCombinatin.put(a,temp);
            } else {
                HashSet<Integer> temp = new HashSet<>();
                temp.add(v);
                subgraphCombinatin.put(a,temp);
            }
        }




        for (Map.Entry<Integer, HashSet<Integer>> entry1 : subgraphCombinatin.entrySet()) {
            Integer a1 = entry1.getKey();
            HashSet<Integer> neighbours1 = entry1.getValue();

            for (Map.Entry<Integer, HashSet<Integer>> entry2 : subgraphCombinatin.entrySet()) {
                Integer a2 = entry2.getKey();
                HashSet<Integer> neighbours2 = entry2.getValue();
                for (Map.Entry<Integer, HashSet<Integer>> entry3 : subgraphCombinatin.entrySet()) {
                    Integer a3 = entry3.getKey();
                    HashSet<Integer> neighbours3 = entry3.getValue();

                    HashSet<Integer> sharData12 = UtilClass.shareNeighbours(neighbours1, neighbours2);
                    HashSet<Integer> sharData13 = UtilClass.shareNeighbours(neighbours1, neighbours3);
                    HashSet<Integer> sharData23 = UtilClass.shareNeighbours(neighbours2, neighbours3);

                    for (Integer item1 : sharData12) {
                        for (Integer item2 : sharData13) {
                            for (Integer item3 : sharData23) {
                                if (item1 != item2 && item1 != item3 && item2 != item3) {

                                }
                            }
                        }
                    }

                }
            }
        }



    }

    private static class UtilClass {
        public static HashMap<Integer,HashSet<Integer>> readEdgeSet (FileSystem fs, String dataPathSuffix, Integer index, HashMap<Integer,HashSet<Integer>> subgraph) throws IOException, URISyntaxException, InterruptedException {



            Path edgeSetPath = new Path(dataPathSuffix + "/edgeSet-" + index );

            if (!fs.exists(edgeSetPath)) {
//                System.out.println("no exsist edge path=" + edgeSetPath);
                return subgraph;
            }

            Input in = new Input(new SnappyInputStream(fs.open(edgeSetPath).getWrappedStream()));
            while (true) {
                int u = in.readInt(true);

                if (u == -1) {

                    break;
                }
                int v = in.readInt(true);

                if (subgraph.containsKey(u)){
                    HashSet<Integer> list = subgraph.get(u);
                    list.add(v);
                } else {
                    HashSet<Integer> list = new HashSet<>();
                    list.add(v);
                    subgraph.put(u,list);
                }

            }
            in.close();
            return subgraph;
        }


        public static HashSet<Integer> shareNeighbours(HashSet<Integer> set1 ,HashSet<Integer> set2) {

            HashSet<Integer> sHopNr1And2C = new HashSet<>();

            if (set1.size() < set2.size()) {
                for (Integer item : set1) {
                    if (set2.contains(item)) {
                        sHopNr1And2C.add(item);

                    }
                }
            } else {
                for (Integer item : set2) {
                    if (set1.contains(item)) {
                        sHopNr1And2C.add(item);

                    }
                }
            }
            return sHopNr1And2C;

        }
    }


    // 将子集组合任务发送到指定节点
//    public static void sendChunk(List<Tuple2<Integer,Integer>> chunk, int targetRank) {
//        // 转换组合为二维数组形式
//        int[][] serializedChunk = chunk.stream()
//                .map(list -> list.stream().mapToInt(Integer::intValue).toArray())
//                .toArray(int[][]::new);
//
//        // 发送二维数组数据
//        MPI.COMM_WORLD.Send(serializedChunk, 0, serializedChunk.length, MPI.OBJECT, targetRank, 0);
//    }


    public static void sendChunk(List<Tuple2<Integer, Integer>> chunk, int targetRank) {
            // 转换 Tuple2<Integer, Integer> 为二维数组形式
            int[][] serializedChunk = new int[chunk.size()][2];
            for (int i = 0; i < chunk.size(); i++) {
                Tuple2<Integer, Integer> tuple = chunk.get(i);
                serializedChunk[i][0] = tuple._1(); // 第一个整数
                serializedChunk[i][1] = tuple._2(); // 第二个整数
            }

            // 发送二维数组数据
            MPI.COMM_WORLD.Send(serializedChunk, 0, serializedChunk.length, MPI.OBJECT, targetRank, 0);
        }


    public static List<Tuple2<Integer,Integer>> receiveChunk(int rank) {
        Object[] recvBuffer = new Object[1];
        MPI.COMM_WORLD.Recv(recvBuffer, 0, 1, MPI.OBJECT, 0, 0);

        int[][] serializedChunk = (int[][]) recvBuffer[0];
        List<Tuple2<Integer,Integer>> chunk = new ArrayList<>();
        for (int[] edge : serializedChunk) {


            chunk.add(new Tuple2<>(edge[0],edge[1]));
        }
        return chunk;
    }
}

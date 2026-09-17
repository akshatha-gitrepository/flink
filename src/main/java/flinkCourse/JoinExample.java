package flinkCourse;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;

public class JoinExample {

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // set up the execution environment
        final ParameterTool params = ParameterTool.fromArgs(args);
        //make Parameter available in UI
        env.getConfig().setGlobalJobParameters(params);

        DataSet<Tuple2<Integer,String>> text1 = env.readTextFile(params.get("input"))
                                .map(new MapFunction<String, Tuple2<Integer,String>>() {
                                    @Override
                                    public Tuple2<Integer, String> map(String value) throws Exception {
                                        String[] parts = value.split(",");
                                        return new Tuple2<>(Integer.parseInt(parts[0]), parts[1]);
                                    }
                                });

        DataSet<Tuple2<Integer,String>> text2 = env.readTextFile(params.get("input2"))
                                .map(new MapFunction<String, Tuple2<Integer,String>>() {
                                    @Override
                                    public Tuple2<Integer, String> map(String value) throws Exception {
                                        String[] parts = value.split(",");
                                        return new Tuple2<>(Integer.parseInt(parts[0]), parts[1]);
                                    }
                                });

        //join dataset

        DataSet<Tuple3<Integer,String,String>> joined = text1.join(text2).where(0).equalTo(0)
                                    .with(new JoinFunction<Tuple2<Integer,String>, Tuple2<Integer,String>, Tuple3<Integer,String,String>>() {
                                        @Override
                                        public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                                            return new Tuple3<>(first.f0, first.f1, second.f1);
                                        }
                                    });

        joined.writeAsCsv(params.get("output"), "\n", ",");

        env.execute("Join Example");
    }
}

public class WordCount{
    public static void main(String[] args) {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // set up the execution environment
        final ParameterTool params = ParameterTool.fromArgs(args);
        //make Parameter available in UI
        env.getConfig().setGlobalJobParameters(params);

        DataSet<String> text = env.readTextFile(params.get("input"));

        //filter all names starting with N
        DataSet<String> filtered = text.filter(new FilterFunction<String>(){

            @Override 
            public boolean filter(String value){

                return value.startsWith("N");

            }
        });

        DataSet<Tuple2<String, Integer>> tokenized = filtered.map(new Tokenizer());

        DataSet<Tuple2<String, Integer>> counts = tokenized.groupBy(0).sum(1);

        //emits result
        if(params.has("output")){
            counts.writeAsCsv(params.get("output"), "\n", "");
            //execute
            env.execute("WordCount Example");
        }

    }

    public static final class Tokenizer implements MapFunction<String, Tuple2<String,Integer>>{

        public Tuple2<String, Integer> map(String value){
            return new Tuple2<String, Integer>(value, 1);
        }
    }

    //when you have multiple output from a single input

    public static final class Tokenizer implements flatMapFunction<String, Tuple2<String, Integer>>{

        @Override 
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out){
        //split the line
        String[] tokens = value.split(" ");
        
        for(String token : tokens){
            if(token.length() > 0){
                out.collect(new Tuple2<String, Integer>(token, 1)); // [(noman,1), (Nipun,1),( Fliyos,1)]

            }
        }

        }
    }
}
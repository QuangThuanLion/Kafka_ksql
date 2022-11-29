package com.example.kafkaexample;

import com.example.kafkaexample.ksqlConfig.ExampleAppKsql;
import com.example.kafkaexample.ksqlConfig.RequestDTO;
import com.example.kafkaexample.ksqlConfig.ResponseDTO;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("api/v1/messages")
public class MessageController {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ExampleAppKsql exampleAppKsql;

    public MessageController(KafkaTemplate<String, String> kafkaTemplate, ExampleAppKsql exampleAppKsql) {
        this.kafkaTemplate = kafkaTemplate;
        this.exampleAppKsql = exampleAppKsql;
    }

    @PostMapping
    public String publish(@RequestBody MessageRequest request) throws InterruptedException {
        kafkaTemplate.send("topicConfiguration", request.getMessage());
        return "message can be push on kafka";
    }

    @GetMapping("/ksql")
    public List<ResponseDTO> publish() throws InterruptedException, ExecutionException {
        List<ResponseDTO> result = exampleAppKsql.configKsql();
        return result;
    }

    @GetMapping("/ksql/queryId")
    public String queryId () throws ExecutionException, InterruptedException {
        String result = exampleAppKsql.clientQueryId();
        return result;
    }

    @PostMapping("/ksql/insert")
    public String insertData(@RequestBody RequestDTO requestDTO) throws ExecutionException, InterruptedException {
        exampleAppKsql.insertQuery(requestDTO);
        return "Insert data successfully";
    }

    @GetMapping("/ksql/create_stream")
    public String createStream(@RequestBody String queryString){
        exampleAppKsql.createStream(queryString);
        return "Create Stream successfully";
    }

//    @GetMapping("/count/{word}")
//    public Long getWordCount(@PathVariable String word) {
//        StreamsBuilderFactoryBean factoryBean = new StreamsBuilderFactoryBean();
//        KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();
//        ReadOnlyKeyValueStore<String, Long> counts = kafkaStreams.store(
//                StoreQueryParameters.fromNameAndType("counts", QueryableStoreTypes.keyValueStore())
//        );
//
//        return counts.get(word);
//    }
}

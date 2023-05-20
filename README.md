# Stream_NOAAWeatherData

For this project, imagine a scenario where many weather stations are sending daily records to a Kafka stream. A regular Python program consumes the stream to produce JSON files with summary stats for each station, for use on a web dashboard. A Spark streaming job also consumes the streams to generate datasets for machine learning (the goal being to predict weather). Spark is also used to train models on this data.

In this scenario, many computers would be involved (different stations, many Kafka brokers, etc). For simplicity, we've used a single Kafka broker. A single producer will generate data for all stations at an accelerated rate (1 day per second). Finally, consumers will be different threads, launching from the same notebook.

Learning objectives:

write code for Kafka producers;
write code for Kafka consumers;
apply streaming techniques to achive "exactly once" semantics;
use Spark streaming to consumer and transform data from Kafka;
use Spark to train models;

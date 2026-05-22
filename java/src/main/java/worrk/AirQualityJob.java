package worrk;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

import java.util.Properties;

public class AirQualityJob {

    public static void main(String[] args) throws Exception {

        // =====================================================
        // 1. 创建 Flink 环境
        // =====================================================

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // =====================================================
        // 2. Kafka 配置
        // =====================================================

        Properties props = new Properties();

        props.setProperty(
                "bootstrap.servers",
                "192.168.198.129:9092"
        );

        // 新 group.id
        props.setProperty(
                "group.id",
                "air-quality-group-final"
        );

        // =====================================================
        // 3. Kafka Consumer
        // =====================================================

        FlinkKafkaConsumer<String> consumer =
                new FlinkKafkaConsumer<>(

                        // 新 Topic
                        "air-quality-final",

                        new SimpleStringSchema(),

                        props
                );

        // 从最早开始消费
        consumer.setStartFromEarliest();

        // =====================================================
        // 4. 读取 Kafka 数据流
        // =====================================================

        DataStream<String> stream =
                env.addSource(consumer);

        // =====================================================
        // 5. JSON 解析
        // =====================================================

        DataStream<JSONObject> jsonStream =

                stream.map(new MapFunction<String, JSONObject>() {

                    @Override
                    public JSONObject map(String value)
                            throws Exception {

                        return JSON.parseObject(value);
                    }
                });

        // =====================================================
        // 6. 控制台打印
        // =====================================================

        jsonStream.print();

        // =====================================================
        // 7. 写入 MySQL
        // =====================================================

        jsonStream.addSink(

                new SinkFunction<JSONObject>() {

                    @Override
                    public void invoke(
                            JSONObject data,
                            Context context
                    ) throws Exception {

                        // =================================================
                        // MySQL 连接
                        // =================================================

                        Connection conn =
                                DriverManager.getConnection(

                                        "jdbc:mysql://192.168.198.129:3306/air_quality?useSSL=false&serverTimezone=UTC",

                                        "root",

                                        ""
                                );

                        // =================================================
                        // 获取字段
                        // =================================================

                        String stationId =
                                data.getString("station_id");

                        String measurementTime =
                                data.getString("measurement_time");

                        double co =
                                data.getDoubleValue("co");

                        double no2 =
                                data.getDoubleValue("no2");

                        double temp =
                                data.getDoubleValue("temp");

                        double humidity =
                                data.getDoubleValue("humidity");

                        // =================================================
                        // 1 aggregation_6h
                        // =================================================

                        String aggSql =

                                "INSERT INTO aggregation_6h " +

                                        "(measurement_time, station_id, co_avg, co_max, co_min) " +

                                        "VALUES (?, ?, ?, ?, ?)";

                        PreparedStatement aggPs =
                                conn.prepareStatement(aggSql);

                        aggPs.setString(1, measurementTime);

                        aggPs.setString(2, stationId);

                        aggPs.setDouble(3, co);

                        aggPs.setDouble(4, co + 1);

                        aggPs.setDouble(5, Math.max(0, co - 1));

                        aggPs.executeUpdate();

                        aggPs.close();

                        // =================================================
                        // 2 alert_record
                        // =================================================

                        if (co > 3) {

                            String level = "WARNING";

                            if (co > 5) {

                                level = "CRITICAL";
                            }

                            String alertSql =

                                    "INSERT INTO alert_record " +

                                            "(alert_time, station_id, pollutant, alert_value, alert_level) " +

                                            "VALUES (?, ?, ?, ?, ?)";

                            PreparedStatement alertPs =
                                    conn.prepareStatement(alertSql);

                            alertPs.setString(1, measurementTime);

                            alertPs.setString(2, stationId);

                            alertPs.setString(3, "CO");

                            alertPs.setDouble(4, co);

                            alertPs.setString(5, level);

                            alertPs.executeUpdate();

                            alertPs.close();
                        }

                        // =================================================
                        // 3 anomaly_detection
                        // =================================================

                        if (co > 5 || no2 > 180) {

                            String anomalySql =

                                    "INSERT INTO anomaly_detection " +

                                            "(detect_time, station_id, pollutant, anomaly_value, anomaly_type) " +

                                            "VALUES (?, ?, ?, ?, ?)";

                            PreparedStatement anomalyPs =
                                    conn.prepareStatement(anomalySql);

                            anomalyPs.setString(1, measurementTime);

                            anomalyPs.setString(2, stationId);

                            anomalyPs.setString(3, "CO");

                            anomalyPs.setDouble(4, co);

                            anomalyPs.setString(5, "HIGH_POLLUTION");

                            anomalyPs.executeUpdate();

                            anomalyPs.close();
                        }

                        // =================================================
                        // 4 sliding_window_trend
                        // =================================================

                        String trendSql =

                                "INSERT INTO sliding_window_trend " +

                                        "(window_end, station_id, co_avg) " +

                                        "VALUES (?, ?, ?)";

                        PreparedStatement trendPs =
                                conn.prepareStatement(trendSql);

                        trendPs.setString(1, measurementTime);

                        trendPs.setString(2, stationId);

                        trendPs.setDouble(3, co);

                        trendPs.executeUpdate();

                        trendPs.close();

                        // =================================================
                        // 5 source_contribution
                        // =================================================

                        double ratio = 0;

                        if (no2 != 0) {

                            ratio = co / no2;
                        }

                        String sourceSql =

                                "INSERT INTO source_contribution " +

                                        "(calc_time, station_id, co_no2_ratio, source_type) " +

                                        "VALUES (?, ?, ?, ?)";

                        PreparedStatement sourcePs =
                                conn.prepareStatement(sourceSql);

                        sourcePs.setString(1, measurementTime);

                        sourcePs.setString(2, stationId);

                        sourcePs.setDouble(3, ratio);

                        sourcePs.setString(4, "MOBILE");

                        sourcePs.executeUpdate();

                        sourcePs.close();

                        // =================================================
                        // 6 weather_correlation
                        // =================================================

                        double corr =
                                co / (temp + humidity + 1);

                        String weatherSql =

                                "INSERT INTO weather_correlation " +

                                        "(calc_time, station_id, temp_co_corr) " +

                                        "VALUES (?, ?, ?)";

                        PreparedStatement weatherPs =
                                conn.prepareStatement(weatherSql);

                        weatherPs.setString(1, measurementTime);

                        weatherPs.setString(2, stationId);

                        weatherPs.setDouble(3, corr);

                        weatherPs.executeUpdate();

                        weatherPs.close();

                        // =================================================
                        // 关闭连接
                        // =================================================

                        conn.close();
                    }
                }
        );

        // =====================================================
        // 8. 启动 Flink
        // =====================================================

        env.execute("Air Quality Real-time Full Analysis");
    }
}
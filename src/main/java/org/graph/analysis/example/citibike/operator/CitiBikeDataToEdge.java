package org.graph.analysis.example.citibike.operator;

import org.apache.flink.util.Collector;
import org.graph.analysis.utils.CSVParser;
import org.graph.analysis.example.citibike.entity.Bike;
import org.graph.analysis.example.citibike.entity.RelationLabel;
import org.graph.analysis.example.citibike.entity.Station;
import org.graph.analysis.example.citibike.entity.User;
import org.graph.analysis.entity.Edge;
import org.graph.analysis.operator.StreamToGraph;
import org.graph.analysis.entity.Vertex;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

public class CitiBikeDataToEdge implements StreamToGraph<String> {

    /**
     * value format: "tripduration","starttime","stoptime","start station id","start station name",
     * "start station latitude","start station longitude","end station id","end station name","end station latitude",
     * "end station longitude","bikeid","usertype","birth year","gender"
     */
    @Override
    public void flatMap(String value, Collector<Edge<Vertex, Vertex>> out) throws Exception {
        CSVParser csvParser = new CSVParser();
        List<String> strings = csvParser.fromCSVLinetoArray(value);

        Vertex startStation = new Station(strings.get(3), strings.get(4), strings.get(5), strings.get(6));
        Vertex endStation = new Station(strings.get(7), strings.get(8), strings.get(9), strings.get(10));
        Vertex bike = new Bike(strings.get(11));
        Vertex user = new User(strings.get(12), strings.get(13), strings.get(14));

        String edgeId = getUUID();
        String startTime = strings.get(1);
        String stopTime = strings.get(2);
        Long startTimeStamp = getTimeStamp(startTime);
        Long stopTimeStamp = getTimeStamp(stopTime);


        HashMap<String, String> rideProperties = new HashMap<>();
        rideProperties.put("startTime", strings.get(1));
        rideProperties.put("stopTime", strings.get(2));
        rideProperties.put("tripDuration", strings.get(0));
        out.collect(new Edge<>(user, bike, RelationLabel.Ride.getLabel(), edgeId, stopTimeStamp, rideProperties));
        HashMap<String, String> otherEdgeProperties = new HashMap<>();
        otherEdgeProperties.put("rideId", edgeId);

        out.collect(new Edge<>(user, startStation, RelationLabel.TripFrom.getLabel(), getUUID(), startTimeStamp, otherEdgeProperties));
        out.collect(new Edge<>(user, endStation, RelationLabel.TripTo.getLabel(), getUUID(), stopTimeStamp, otherEdgeProperties));
        out.collect(new Edge<>(bike, startStation, RelationLabel.RiddenFrom.getLabel(), getUUID(), startTimeStamp, otherEdgeProperties));
        out.collect(new Edge<>(bike, startStation, RelationLabel.RiddenTo.getLabel(), getUUID(), stopTimeStamp, otherEdgeProperties));

    }

    private Long getTimeStamp(String timeStr) {
        Date date = new Date();
        //注意format的格式要与日期String的格式相匹配
        DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
        try {
            Timestamp ts = Timestamp.valueOf(timeStr);
            return ts.getTime();
        } catch (Exception e) {
            e.printStackTrace();
            return System.currentTimeMillis();
        }
    }

    private String getUUID() {
        return UUID.randomUUID().toString().replace("-", "").toUpperCase();
    }
}

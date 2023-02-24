package flights.topology;

import flight.FlightKpi;
import flights.serde.Serde;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import radar.AirportKpi;
import radar.AirportUpdateEvent;
import radar.FlightUpdateEvent;

public class TopologyBuilder implements Serde {

    private Properties config;

    public TopologyBuilder(Properties properties) {
        this.config = properties;
    }

    private static final Logger logger = LogManager.getLogger(TopologyBuilder.class);



    public Topology build() {
        StreamsBuilder builder = new StreamsBuilder();
        String schemaRegistry = config.getProperty("kafka.schema.registry.url");

        KStream<String, FlightUpdateEvent> flightInputStream = builder.stream(
                config.getProperty("kafka.topic.flight.update.events"),
                Consumed.with(Serde.stringSerde, Serde.specificSerde(FlightUpdateEvent.class, schemaRegistry)));

       // flightInputStream.filter((s, flightUpdateEvent) ->  !flightUpdateEvent.getStatus().equals("CANCELED"));
        KStream<String, FlightKpi> outputStream = flightInputStream.mapValues(new ValueMapperWithKey<String, FlightUpdateEvent, FlightKpi>() {

            @Override
            public FlightKpi apply(String s, FlightUpdateEvent flightUpdateEvent) {
                TimeZone tz = TimeZone.getTimeZone("UTC");
                DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
                df.setTimeZone(tz);
                //Stockholm,Sweden(ESKB)->Nice,France(LFMN)"
                String departure = flightUpdateEvent.getDestination().toString().split("->")[0];
                String arrival = flightUpdateEvent.getDestination().toString().split("->")[1];

                String departureCode = (departure.split("\\(")[1]);
                String arrivalCode = arrival.split("\\(")[1];

                return FlightKpi.newBuilder().setTo(arrival.split("\\(")[0])
                        .setFrom(departure.split("\\(")[0])
                        .setDepartureTimestamp(flightUpdateEvent.getSTD())
                        .setArrivalTimestamp(flightUpdateEvent.getSTA())
                        .setDuration((flightUpdateEvent.getSTA()-flightUpdateEvent.getSTD())/60)
                        .setDepartureDatetime(df.format(new Timestamp(flightUpdateEvent.getSTD())))
                        .setArrivalDatetime(df.format(new Timestamp(flightUpdateEvent.getSTA())))
                        .setDepartureAirportCode(departureCode.substring(0, departureCode.length()-1))
                        .setArrivalAirportCode(arrivalCode.substring(0, arrivalCode.length() - 1))
                        .setAirline(flightUpdateEvent.getAirline())
                        .setGate(flightUpdateEvent.getGate())
                        .setStatus(flightUpdateEvent.getStatus())
                        .setDate(flightUpdateEvent.getDate())
                        .setId(flightUpdateEvent.getId())
                        .build();
            }
        });
        Predicate<String, FlightKpi> filterCanceled = (key, value) -> !value.getStatus().equals("CANCELED");
        outputStream = outputStream.filter(filterCanceled);
        outputStream.to(config.getProperty("kafka.topic.radar.flights"), Produced.with(Serde.stringSerde, Serde.specificSerde(FlightKpi.class, schemaRegistry)));


        GlobalKTable<String, AirportUpdateEvent> airportTable = builder.globalTable(
                config.getProperty("kafka.topic.airport.update.events"),
                Consumed.with(Serde.stringSerde, Serde.specificSerde(AirportUpdateEvent.class, schemaRegistry)));

        return builder.build();
    }
}

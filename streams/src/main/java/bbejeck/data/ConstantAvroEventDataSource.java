package bbejeck.data;

import bbejeck.chapter_4.avro.DeliveryEvent;
import bbejeck.chapter_4.avro.PlaneEvent;
import bbejeck.chapter_4.avro.TruckEvent;
import org.apache.avro.specific.SpecificRecord;

import java.util.Collection;
import java.util.List;

/**
 * A implementation of the {@link DataSource} interface that
 * supplies the same three Avro events with each call to {@link  DataSource#fetch()}
 * useful for testing purposes
 */
public class ConstantAvroEventDataSource implements DataSource<SpecificRecord> {

    @Override
    public Collection<SpecificRecord> fetch() {
        
        TruckEvent truckEvent = TruckEvent.newBuilder()
                .setId("customer-1")
                .setPackageId("1234XRTY")
                .setWarehouseId("Warehouse63")
                .setTime(500).build();

        PlaneEvent planeEvent = PlaneEvent.newBuilder()
                .setId("customer-1")
                .setPackageId("1234XRTY")
                .setAirportCode("DCI")
                .setTime(600).build();


        DeliveryEvent deliveryEvent = DeliveryEvent.newBuilder()
                .setId("customer-1")
                .setPackageId("1234XRTY")
                .setCustomerId("Vandley034")
                .setTime(700).build();

        return List.of(truckEvent, planeEvent,deliveryEvent);
    }
}

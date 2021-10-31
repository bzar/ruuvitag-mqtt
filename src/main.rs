use std::error::Error;
use futures::stream::StreamExt;
use tokio::task;
use btleplug::{
    api::{BDAddr, Central, CentralEvent, Manager as _},
    platform::Manager
};
use ruuvi_sensor_protocol::{SensorValues, Humidity, Temperature, Pressure, BatteryPotential, Acceleration};
use clap::Parser;
use rumqttc::{MqttOptions, AsyncClient, QoS};

#[macro_use] extern crate log;


#[derive(Parser)]
#[clap(version = "1.0", author = "Teemu Erkkola <teemu.erkkola@iki.fi>")]
struct Opts {
    #[clap(short, long, default_value = "ruuvitag-mqtt")]
    name: String,
    #[clap(short, long, default_value = "localhost")]
    host: String,
    #[clap(short, long, default_value = "1883")]
    port: u16,
    #[clap(short, long, default_value = "sensor/ruuvitag")]
    base_topic: String
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Initialize logging
    pretty_env_logger::init();

    // Parse command line parameters
    let opts: Opts = Opts::parse();

    // Find a bluetooth adapter and start scanning
    let adapter = {
        let manager = Manager::new().await?;
        let adapters = manager.adapters().await?;
        // TODO: smarter way to pick an adapter, use the first one for now
        if let Some(adapter) = adapters.into_iter().next() {
            adapter
        } else {
            error!("No bluetooth adapters detected");
            return Ok(())
        }
    };
    let mut events = adapter.events().await?;
    adapter.start_scan().await?;

    // Connect to MQTT server
    let (mut client, mut mqtt_events) = {
        let mut mqttoptions = MqttOptions::new(&opts.name, &opts.host, opts.port);
        mqttoptions.set_keep_alive(5);
        AsyncClient::new(mqttoptions, 10)
    };

    // Initial connection check
    match mqtt_events.poll().await {
        Ok(_) => info!("Connected to MQTT server at {}:{}", opts.host, opts.port),
        Err(e) => {
            error!("Error connecting to MQTT server at {}:{}: {}", opts.host, opts.port, e);
            return Ok(())
        }
    };

    // Spawn a task for MQTT eventloop
    task::spawn(async move {
        const MAX_WAIT_DURATION: u64 = 60;
        let mut wait_duration = 1;
        loop {
            match mqtt_events.poll().await {
                Ok(_) => {
                    trace!("MQTT event loop poll success");
                    wait_duration = 1;
                },
                Err(e) => {
                    error!("MQTT event loop poll failed: {}, waiting {}s before retry", e, wait_duration);
                    tokio::time::sleep(tokio::time::Duration::from_secs(wait_duration)).await;
                    wait_duration = (wait_duration + 1).min(MAX_WAIT_DURATION);
                }
            }
        }
    });

    // Bluetooth eventloop
    while let Some(event) = events.next().await {
        if let Some((address, sensor_values)) = event_to_sensor_values(event).await {
            match post_sensor_values(&opts.base_topic, address, sensor_values, &mut client).await {
                Ok(_) => debug!("Values published successfully"),
                Err(e) => error!("Publishing values failed: {}", e)
            };
        }
    }

    info!("Bluetooth event loop terminated, exiting.");
    Ok(())
}

async fn event_to_sensor_values(event: CentralEvent) -> Option<(BDAddr, SensorValues)> {
    if let CentralEvent::ManufacturerDataAdvertisement { address, manufacturer_data } = event {
        // TODO: Pick the correct manufacturer data, for now assume it's the only one
        manufacturer_data.into_iter().next().map(|(id, data)| {
            match SensorValues::from_manufacturer_specific_data(id, &data) {
                Ok(sensor_values) => Some((address, sensor_values)),
                Err(e) => {
                    debug!("Ignoring malformed data from {:?}: {:?}", address, e);
                    None
                }
            }
        }).flatten()
    } else {
        None
    }
}

async fn post_sensor_values(base_topic: &str, address: BDAddr, sensor_values: SensorValues, client: &mut AsyncClient) -> Result<(), Box<dyn Error>> {
    info!("Sensor data from {:?}: {:?}", address, sensor_values);

    let values = [
        ("temperature", sensor_values.temperature_as_millicelsius().map(|v| v as f32 / 1000.0)),
        ("humidity", sensor_values.humidity_as_ppm().map(|v| v as f32 / 10000.0)),
        ("pressure", sensor_values.pressure_as_pascals().map(|v| v as f32 / 100.0)),
        ("battery", sensor_values.battery_potential_as_millivolts().map(|v| v as f32 / 1000.0)),
        ("acceleration_x", sensor_values.acceleration_vector_as_milli_g().map(|v| v.0 as f32 / 1000.0)),
        ("acceleration_y", sensor_values.acceleration_vector_as_milli_g().map(|v| v.1 as f32 / 1000.0)),
        ("acceleration_z", sensor_values.acceleration_vector_as_milli_g().map(|v| v.2 as f32 / 1000.0)),
    ];

    let sensor_topic = format!("{}/{}", base_topic, address);
    let mut json_data = json::JsonValue::new_object();

    for (value_name, value) in values {
        json_data[value_name] = value.into();
        if let Some(value) = value {
            let topic = format!("{}/{}", sensor_topic, value_name);
            client.publish(&topic, QoS::AtLeastOnce, true, value.to_string()).await?;
        }
    }

    client.publish(&sensor_topic, QoS::AtLeastOnce, true, json_data.dump()).await?;

    Ok(())
}

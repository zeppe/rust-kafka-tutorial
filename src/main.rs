use std::env::args;
use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::util::Timeout;
use rdkafka::Message;
use uuid::Uuid;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};


fn create_producer(bootstrap_server: &str) -> FutureProducer {
    ClientConfig::new()
    .set("bootstrap.servers", bootstrap_server)
    .set("queue.buffering.max.ms", "0")
    .create().expect("Failed to create client")
}

fn create_consumer(bootstrap_server: &str) -> StreamConsumer {
    ClientConfig::new()
    .set("bootstrap.servers", bootstrap_server)
    .set("enable.partition.eof", "false")
    // We'll give each session its own (unique) consumer group id,
    // so that each session will receive all messages
    .set("group.id", format!("chat-{}", Uuid::new_v4()))
    .create()
    .expect("Failed to create client")
}

async fn write_prompt(out: &mut tokio::io::Stdout) {
    // Write a prompt to stdout
    out.write(b"> ").await.unwrap();
    out.flush().await.unwrap();
}

#[tokio::main] // starts the Tokio runtime for async
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut stdout = tokio::io::stdout();
    
    stdout.write(b"Welcome to Kafka chat!\n").await.unwrap();
    
    // Creates a producer, reading the bootstrap server from the first command-line argument
    // or defaulting to localhost:9092
    let producer = create_producer(&args().skip(1).next()
    .unwrap_or("localhost:9092".to_string()));
    
    // create the consumer
    let consumer = create_consumer(&args().skip(1).next()
    .unwrap_or("localhost:9092".to_string()));
    
    // subscribe to our topic
    consumer.subscribe(&["chat"]).unwrap();
    
    let mut stdout = tokio::io::stdout();
    let mut input_lines = BufReader::new(tokio::io::stdin()).lines();
    
    let name;
    loop {
        stdout.write(b"Please enter your name: ").await?;
        stdout.flush().await?;
 
        if let Some(s) = input_lines.next_line().await? {
            if s.is_empty() {
                continue;
            }
 
            name = s;
            break;
        }
    };

    producer.send(FutureRecord::to("chat")
        .key(&name)
        .payload(b"has joined the chat"), Timeout::Never)
        .await
        .expect("Failed to produce");

    write_prompt(&mut stdout).await;
    loop {
        tokio::select! {
            message = consumer.recv() => {
                let message  = message.expect("Failed to read message").detach();
                let key = message.key().ok_or_else(|| "no key for message")?;
 
                if key == name.as_bytes() {
                    continue;
                }
                let payload = message.payload().unwrap();
                stdout.write(b"\t").await?;
                stdout.write(key).await?;
                stdout.write(b": ").await?;
                stdout.write(payload).await?;
                stdout.write(b"\n").await?;
                write_prompt(&mut stdout).await;
            }
            
            // Read a line from stdin
            line = input_lines.next_line() => {
                match line {
                    Ok(Some(line)) => {
                        // Send the line to Kafka on the 'chat' topic
                        producer.send(FutureRecord::to("chat")
                        .key(&name)
                        .payload(&line), Timeout::Never)
                        .await
                        .map_err(|(e, _)| format!("Failed to produce: {:?}", e))?;
                        write_prompt(&mut stdout).await;
                    }
                    _ => break,
                }
            }
            
        }
    }

    Ok(())
}
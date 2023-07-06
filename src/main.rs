use std::time::Duration;
use std::{num::NonZeroUsize, thread};

use clap::{Parser, ValueEnum};
use redis::Commands;

#[derive(Debug, Clone, Copy, ValueEnum)]
enum StoreCommand {
    Rpush,
}

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(help = "redis host")]
    hostname: String,

    #[arg(value_enum, help = "redis command to store the line")]
    store_command: StoreCommand,

    #[arg(help = "key to store the line into")]
    key: String,

    #[arg(short, long, default_value_t = 5, help = "timeout in seconds")]
    timeout: u64,

    #[arg(
        short,
        long,
        default_value_t = 5,
        help = "time to wait before reconnecting in seconds"
    )]
    delay: u64,

    #[arg(short, long, default_value_t = NonZeroUsize::new(1024).unwrap())]
    queue_size: NonZeroUsize,
}

#[derive(Debug)]
enum FeederMessage {
    Line(String),
    Quit,
}

struct RedisFeeder {
    client: redis::Client,
    conn: redis::Connection,
    timeout: Duration,
    key: String,
    store_command: StoreCommand,

    reconnect_delay: Duration,
}

impl RedisFeeder {
    pub fn new(
        client: redis::Client,
        timeout: Duration,
        key: String,
        store_command: StoreCommand,
        reconnect_delay: Duration,
    ) -> Self {
        let conn = try_connecting_until_successful(&client, timeout, reconnect_delay);

        log::info!("Connected");

        Self {
            client,
            conn,
            timeout,
            key,
            store_command,
            reconnect_delay,
        }
    }

    pub fn feed(&mut self, line: &str) {
        while let Err(err) = self.store(line) {
            if err.is_connection_dropped()
                || err.is_connection_refusal()
                || err.is_io_error()
                || err.is_timeout()
            {
                log::warn!("Connection to redis failed: {err}, reconnecting");
                self.reconnect();
            } else {
                log::error!("Storing line in redis failed: {err}");
                break;
            }
        }
    }

    /// Store `line` in redis using `self.store_command`.
    fn store(&mut self, line: &str) -> redis::RedisResult<()> {
        match self.store_command {
            StoreCommand::Rpush => self.conn.rpush(self.key.as_str(), line),
        }
    }

    fn reconnect(&mut self) {
        self.conn =
            try_connecting_until_successful(&self.client, self.timeout, self.reconnect_delay);
        log::info!("Reconnected");
    }
}

fn try_connecting_until_successful(
    client: &redis::Client,
    timeout: Duration,
    retry_delay: Duration,
) -> redis::Connection {
    loop {
        match client.get_connection_with_timeout(timeout) {
            Ok(conn) => break conn,
            Err(err) => {
                log::warn!("Connect failed: {err}, trying again...");
                thread::sleep(retry_delay);
            }
        }
    }
}

fn feeder_thread(mut recv: ring_channel::RingReceiver<FeederMessage>, mut feeder: RedisFeeder) {
    while let Ok(msg) = recv.recv() {
        match msg {
            FeederMessage::Line(line) => feeder.feed(&line),
            FeederMessage::Quit => break,
        }
    }

    log::info!("Feeder thread shutting down.");
}

fn main() -> anyhow::Result<()> {
    env_logger::init();

    let args = Args::parse();

    let conn_str = format!("redis://{}", args.hostname);
    let client = redis::Client::open(conn_str)?;

    let (mut sender, recv) = ring_channel::ring_channel(args.queue_size);

    let t = thread::spawn(move || {
        let feeder = RedisFeeder::new(
            client,
            Duration::from_secs(args.timeout),
            args.key.clone(),
            args.store_command,
            Duration::from_secs(args.delay),
        );

        feeder_thread(recv, feeder);
    });

    for line in std::io::stdin().lines() {
        sender.send(FeederMessage::Line(line?))?;
    }

    sender.send(FeederMessage::Quit)?;
    t.join().unwrap();

    Ok(())
}

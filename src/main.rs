use std::io::Read;
use std::os::unix::process::CommandExt;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand};

use nqueue::broker::{Broker, StartAt};
use nqueue::policy::{Config, Urgency};
use nqueue::valkey;

/// Queue system for agent-to-agent communication.
#[derive(Parser)]
#[command(version)]
struct Cli {
    /// Valkey to connect to [default: unix socket ~/.nqueue/valkey.sock].
    #[arg(long, env = "NQUEUE_VALKEY", global = true)]
    valkey: Option<String>,

    /// The agent to act as.
    #[arg(long = "as", env = "NQUEUE_ID", global = true)]
    id: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run nqueue's Valkey server on ~/.nqueue/valkey.sock, persisting to
    /// ~/.nqueue.
    Daemon,
    /// Bring an agent online and print its ID: the given one, else --as, else
    /// a newly generated one.
    Register { id: Option<String> },
    /// Take the agent offline. Its subscriptions are kept.
    Unregister,
    /// Create a queue, with a policy from the config files.
    Create {
        queue: String,
        /// Policy name; defaults to the queue's [queues.<name>] entry.
        #[arg(long)]
        policy: Option<String>,
    },
    /// Remove a queue and its messages.
    Destroy { queue: String },
    /// Subscribe the agent to queues.
    Subscribe {
        #[arg(required = true)]
        queues: Vec<String>,
        /// Receive broadcast messages already in the queue.
        #[arg(long)]
        from_start: bool,
    },
    /// Unsubscribe the agent from queues.
    Unsubscribe {
        #[arg(required = true)]
        queues: Vec<String>,
    },
    /// Enqueue a message and print its ID. The body is read from stdin if
    /// not given.
    Enqueue {
        queue: String,
        body: Option<String>,
        #[arg(long, conflicts_with = "non_urgent")]
        urgent: bool,
        #[arg(long)]
        non_urgent: bool,
    },
    /// Deliver the agent's waiting messages as JSON lines, leasing those from
    /// at-least-once queues.
    Dequeue {
        #[arg(long)]
        urgent_only: bool,
        #[arg(long)]
        max: Option<usize>,
    },
    /// Wait until messages are waiting for the agent. Exits 0 when there are,
    /// 3 on timeout.
    Wait {
        #[arg(long)]
        urgent_only: bool,
        /// Seconds to wait; forever if unset.
        #[arg(long)]
        timeout: Option<f64>,
    },
    /// Mark a leased message as handled.
    Ack { message_id: String },
    /// Return a leased message to its queue.
    Nack { message_id: String },
    /// Show queues and agents as JSON.
    Status,
    /// Print the policies and queues configured for this directory.
    Config,
}

const EXIT_TIMEOUT: u8 = 3;

fn main() -> ExitCode {
    match run(Cli::parse()) {
        Ok(code) => code,
        Err(e) => {
            eprintln!("nqueue: {e:#}");
            ExitCode::FAILURE
        }
    }
}

fn run(cli: Cli) -> anyhow::Result<ExitCode> {
    let id = || cli.id.clone().ok_or_else(|| anyhow::anyhow!("no agent ID: pass --as or set NQUEUE_ID"));
    let config = || Config::resolve(Some(&valkey::home_config()), &std::env::current_dir()?);

    match &cli.command {
        Command::Daemon => {
            let home = valkey::home_dir();
            std::fs::create_dir_all(&home)?;
            let err = valkey::server_command(&home, &valkey::default_socket(), true).exec();
            anyhow::bail!("running {}: {err}", valkey::server_binary().display());
        }
        Command::Config => {
            println!("{}", toml::to_string_pretty(&config()?)?);
            return Ok(ExitCode::SUCCESS);
        }
        _ => {}
    }

    let url = cli.valkey.clone().unwrap_or_else(valkey::url);
    let mut b = Broker::connect(&url)
        .map_err(|e| anyhow::anyhow!("connecting to {url}: {e} (is `nqueue daemon` running?)"))?;

    match cli.command {
        Command::Daemon | Command::Config => unreachable!(),
        Command::Register { id } => println!("{}", b.register(id.or(cli.id.clone()).as_deref())?),
        Command::Unregister => b.unregister(&id()?)?,
        Command::Create { queue, policy } => {
            let (policy_name, policy) = config()?.policy_for(&queue, policy.as_deref())?;
            b.create_queue(&queue, &policy_name, policy)?;
        }
        Command::Destroy { queue } => b.destroy_queue(&queue)?,
        Command::Subscribe { queues, from_start } => {
            let start = if from_start { StartAt::Beginning } else { StartAt::Now };
            for q in queues {
                b.subscribe(&id()?, &q, start)?;
            }
        }
        Command::Unsubscribe { queues } => {
            for q in queues {
                b.unsubscribe(&id()?, &q)?;
            }
        }
        Command::Enqueue { queue, body, urgent, non_urgent } => {
            let body = match body {
                Some(b) if b != "-" => b,
                _ => {
                    let mut b = String::new();
                    std::io::stdin().read_to_string(&mut b)?;
                    b
                }
            };
            let urgency = match (urgent, non_urgent) {
                (true, _) => Some(Urgency::Urgent),
                (_, true) => Some(Urgency::NonUrgent),
                _ => None,
            };
            println!("{}", b.enqueue(&id()?, &queue, &body, urgency)?);
        }
        Command::Dequeue { urgent_only, max } => {
            for d in b.dequeue(&id()?, urgent_only, max)? {
                println!("{}", serde_json::to_string(&d)?);
            }
        }
        Command::Wait { urgent_only, timeout } => {
            if !b.wait(&id()?, urgent_only, timeout.map(Duration::from_secs_f64))? {
                return Ok(ExitCode::from(EXIT_TIMEOUT));
            }
        }
        Command::Ack { message_id } => b.ack(&id()?, &message_id)?,
        Command::Nack { message_id } => b.nack(&id()?, &message_id)?,
        Command::Status => println!("{}", serde_json::to_string_pretty(&b.status()?)?),
    }
    Ok(ExitCode::SUCCESS)
}
